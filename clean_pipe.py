#!/usr/bin/env python

"""

Code that cleans up local buffers based on triggers that say the remote process is finished.

1) Cleans local '.done' files which are generated once the pull from remote suceeds. 
The remote site uses the list of '.done' files to clean its source buffer.  This code, when run with
"--clean-local-status", compares the remote source buffer for matched '.mrk' files with its local '.done' files.
If no match is found, the remote site has removed the source file allowing the local status file to be removed.

2) Cleans local source files once the remote transfer is confirmed to succeed.  The code, when run with
"--clean-local-buffer", chechs the remote tranfer status area for a matching '.done' file.  When it is found,
the local buffer's target file and '.mrk' file are removed.


"""

import sys
if sys.version[0:3] < '2.6':
    print "Python version 2.6 or greater required (found: %s)." % \
        sys.version[0:5]
    sys.exit(-1)


import math, os, pprint, re, shlex, shutil, socket, stat, time
from datetime import datetime
from signal import alarm, signal, SIGALRM, SIGKILL, SIGTERM
from subprocess import Popen, PIPE, STDOUT
import argparse
from ConfigParser import RawConfigParser
from process_commands import process_commands
import json

#---- Gobal defaults ---- Can be overwritten with commandline arguments 

REMOTE_URL = "gsiftp://stargrid02.rcf.bnl.gov/"
LOCAL_URL = "gsiftp://starreco@dtn06.nersc.gov/"
REMOTE_DIR = "/star/data97/GRID/Cori/"
REMOTE_STATUS = "/star/data97/GRID/status/"
TRANSFER_DIR = "/global/cscratch1/sd/starreco/data/raw/buffer/"
TRANSFER_STATUS_DIR = "/global/cscratch1/sd/starreco/data/raw/trans_status/"
LOCAL_BUFFER = "/global/cscratch1/sd/starreco/data/reco/buffer/"

FTYPE = "daq"
MRKTYPE = "mrk"
DONETYPE="done"

#----------------------------------------

class pipecleaner:
    """ application class """

    def __init__(self, args):
        self.args = args
        self.remote_dir=args.remote_dir
        self.remote_url=args.remote_url
        self.local_url=args.local_url
        self.trans_dir=args.trans_dir
        self.trans_status=args.trans_status
        self.local_buffer=args.local_buffer
        self.remote_status = args.remote_status
        self.ftype=args.ftype
        self.mtype=args.mtype
        self.done=DONETYPE
        self.clean_local_status = args.clean_local_status
        self.clean_local_buffer = args.clean_local_buffer
        self.proc_c = process_commands(args.verbosity)

#-----------------------------------
    def check_proxy(self):
        """ Check for valid proxy, return True/False"""
        s, _o, _e = self.proc_c.comm("grid-proxy-info -e", shell=True,ignore_dry_run=True)
        return s == 0

    def _remote_dirlist(self,rdir):
        cmd = "globus-url-copy -list %s" % "/".join([self.remote_url,rdir])
        self.proc_c.log(" Command:: '%s'" % (cmd), 1)
        s, o, e = self.proc_c.comm(cmd)
        return s, o

    def nextRemoteFile(self,rdir,ltype):
        s, o = self._remote_dirlist(rdir)
        if s == 0:
            flist = o.split()
            for afile in flist:
                if afile.endswith(ltype):
                    yield afile

    def getRemoteFileList(self,rdir,ltype):
        s, o = self._remote_dirlist(rdir)
        retlist = []
        if s == 0:
            flist = o.split()
            for afile in flist:
                if afile.endswith(ltype):
                    retlist.append(afile)
        return retlist

    def nextLocalFile(self,ldir,ltype):
        self.proc_c.log("will search in  %s for %s" % (ldir,ltype),1)
        for root, dirs, files in os.walk(ldir):
            for xfile in files:
                if xfile.endswith(ltype):
                    yield xfile

    def getLocalFileList(self,ldir,ltype):
        retlist = []
        for root, dirs, files in os.walk(ldir):
            for afile in files:
                if afile.endswith(ltype):
                    retlist.append(afile)
        return retlist

    def go(self):

        while True:
            if not self.check_proxy():
                self.proc_c.log("No valid proxy at Time=%s" % datetime.now(),0)
                time.sleep(360)
                continue

# clean local status, removes files if remote target file IS NOT in remote transfer dir
            if self.clean_local_status:
                icount=0
                ifailed=0
                self.proc_c.log("getting remote file list from %s" % (self.remote_dir),1)
                remote_list = self.getRemoteFileList(self.remote_dir,self.ftype)
                for tfile in self.nextLocalFile(self.trans_status,self.done):
                    ifound = False
                    for rfile in remote_list:
                        if tfile == ".".join([rfile,self.done]):
                            self.proc_c.log("File still in remote buffer %s" % (rfile), 3)
                            ifound=True
                            break
                    if not ifound:
                        try:
                            icount+=1
                            self.proc_c.log("removing file # %d  %s" % (icount,tfile),0)
                            os.remove("/".join([self.trans_status,tfile]))
                        except:
                            ifailed+=0
                            self.proc_c.log("remove failed %s" % (tfile),0)
                self.proc_c.log("\n ------- \n Removed %d Status Files with %d OS errors \n ------- \n" % (icount,ifailed),0)


# clean local buffer, removes files if remote done file IS found in remote status dir
            if self.clean_local_buffer:
                icount=0
                ifailed=0
                local_list = self.getLocalFileList(self.local_buffer,self.ftype)
                for tfile in self.nextRemoteFile(self.remote_status,self.done):
                    for rfile in local_list:
                        if tfile == ".".join([rfile, self.done]):
                            try:
                                remove_file="/".join([self.local_buffer,rfile])
                                remove_mfile=".".join([remove_file,self.mtype])
                                self.proc_c.log("Will remove files %s and %s" % (remove_file,remove_mfile),1)
                                os.remove(remove_file)
                                os.remove(remove_mfile)
                                icount+=1
                            except:
                                ifailed+=1
                                self.proc_c.log("remove failed %s" % (rfile),0)
                            break
                self.proc_c.log("\n ------- \n Removed %d Transfer Files with %d OS errors \n ------- \n" % (icount,ifailed),0)
            time.sleep(360)


def main():
    """ Generic program structure to parse args, initialize and start application """
    desc = """ Clean up of the pipeline tool """
    
    p = argparse.ArgumentParser(description=desc, epilog="None")
    p.add_argument("--remote-url",dest="remote_url",default=REMOTE_URL,help="gsiftp url of the remote endpoint")
    p.add_argument("--local-url",dest="local_url",default=LOCAL_URL,help="gsiftp url of the remote endpoint")
    p.add_argument("--remote-dir",dest="remote_dir",default=REMOTE_DIR,help="remote directory data is pulled from")
    p.add_argument("--trans-dir",dest="trans_dir",default=TRANSFER_DIR,help="local directory to store data")
    p.add_argument("--trans-status",dest="trans_status",default=TRANSFER_STATUS_DIR,help="directory to store status of tranfers")
    p.add_argument("--local-buffer",dest="local_buffer",default=LOCAL_BUFFER,help="local directory for remote tranfers")
    p.add_argument("--remote-status",dest="remote_status",default=REMOTE_STATUS,help="remote store status of tranfers")
    p.add_argument("--file-type",dest="ftype",default=FTYPE,help="file extention of data files to be transfered")
    p.add_argument("--marker-type",dest="mtype",default=MRKTYPE,help="file extention of the marker file to be transfered")
    p.add_argument("-v", "--verbose", action="count", dest="verbosity", default=0,                                                                                                 help="be verbose about actions, repeatable")
    p.add_argument("--config-file",dest="config_file",default="None",help="override any configs via a json config file")

    p.add_argument("--clean-local-status", action="store_true", dest="clean_local_status", default=False,
                    help="Clean up local status files after remote buffer has been cleaned")
    p.add_argument("--clean-local-buffer", action="store_true", dest="clean_local_buffer", default=False,
                    help="Clean up local buffer after files have been pulled")


    args = p.parse_args()

#-------- parse config file to override input and defaults
    val=vars(args)
    if not args.config_file == "None":
        try:
            print "opening ", args.config_file
            with open(args.config_file) as config_file:
                configs=json.load(config_file)
            for key in configs:
                print "Thekey is ", key
                if key in val:
                    print "evaluating key ",key
                    if isinstance(configs[key],unicode):
                        val[key]=configs[key].encode("ascii")
                    else:
                        val[key]=configs[key]
        except:
            p.error(" Could not open or parse the configfile ")
            return -1

    try:
        pc = pipecleaner(args)
        return(pc.go())
    except (Exception), oops:
        if args.verbosity >= 2:
            import traceback
            traceback.print_exc()
        else:
            print oops
            return -1
                                                                                                                                                                
if __name__ == "__main__":                      
    sys.exit(main())


