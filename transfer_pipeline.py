#!/usr/bin/env python

"""

Transfer STAR data from RCF to NERSC or from NERSC to RCF.  Based loosely on data_mover.py code written by K.Beattie & J.Porter 

Model is that code searches a remote source directory for a list of target files.  
    With each file it checks that 
        1) a remote MRK file also exists
        2) a local transfer_status file (either ".in_progress" or ".done") does not exist
    If 1) & 2) the code then
        a) creates a local status file as ".in_progress"
        b) tranfers the MRK file
            -) at this point we can read MRK file for information in order to validate transfer
        c) tranfers the target file
            -) on success, it renames status file ".in_progress" to ".done" 
                -) optionally copies it back to the remote source location for use by local site to clean its cache
            -) on failure, it deletes all local files and logs info

    After list of target files is exhausted, it sleeps 5 minutes and re-scans the remote source

    ToDo:  
        Include size information from the MRK file prior to copying target file to set a reasonable timeout
            (we found this useful during network interuptions)

Features:

    With each loop, verifies valid proxy and that there is room on the local cache
    Can run multiple instances concurrently pointing to the same directory structure

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

#-------------------
# Global defaults
#

REMOTE_URL = "gsiftp://stargrid02.rcf.bnl.gov/"
LOCAL_URL = "gsiftp://starreco@dtn06.nersc.gov/"
REMOTE_DIR = "/star/data97/GRID/Cori/"
REMOTE_LIST = "/star/institutions/lbl_prod/transfers/rawfilelist.txt"
TRANSFER_DIR = "/global/cscratch1/sd/starreco/data/raw/buffer/"
TRANSFER_STATUS_DIR = "/global/cscratch1/sd/starreco/data/raw/trans_status/"

STATE_FILE = "transfer_pipeline.state"
TIME_TO_NOTIFY = 86400
PROC_NAME = "transfer_pipeline"
EMAIL_ADDR = "None"

FTYPE = "daq"
MRKTYPE = "mrk"
DONETYPE="done"
DOINGTYPE="in_progress"

BUFFERSIZE = 40000
MIN_BUFFER = 100

GUC_PARALLEL = "8"
HARD_TIMEOUT = 0
RATE_TIMEOUT = 0  # MB/s timeout, 0 => off
MIN_TIMEOUT = 15
MEGABYTES = 1 << 20  # the number of bytes in a MB


class transfer_pipeline:
    """ application class """

    def __init__(self, args):
        self.args = args
        self.remote_dir=args.remote_dir
        self.remote_url=args.remote_url
        self.remote_list= args.remote_list
        self.local_url=args.local_url
        self.trans_dir=args.trans_dir
        self.trans_status=args.trans_status
        self.ftype=args.ftype
        self.mtype=args.mtype
        self.done=DONETYPE
        self.doing=DOINGTYPE
        self.timeout= 0
        self.state_file=args.state_file
        self.ltime=0
        self.time_to_notify=args.time_to_notify
        self.proc_name = PROC_NAME
        self.email_addr = args.email_addr

        self.proc_c = process_commands(args.verbosity)

#        self._logIndent = 0
        self.proc_c.log("opts: %s" % (self.args), 4)


#-----------------------------------
    def check_proxy(self):
        """ Check for valid proxy, return True/False"""
        s, _o, _e = self.proc_c.comm("grid-proxy-info -e", shell=True,ignore_dry_run=True)
        return s == 0

#-----------------------------------
    def check_hold(self):
        """ Check if paused """
        state_file = "/".join([self.trans_status,self.state_file])
        if not os.path.isfile(state_file):
            self.proc_c.log("Cannot find file = %s" % (state_file),2)
            return False
        for aline in open(state_file):
            if aline.split()[0] == "hold":
                return True
        return False

#-----------------------------------
    def notify(self):
        self.held=self.check_hold()
        if self.email_addr=="None":
            return
        mtime=self._unixT()
        if (mtime-self.ltime) > self.time_to_notify:
            emessage="\n".join([" : ".join(["hold",str(self.held)])," : ".join(["date",str(datetime.now())])])
            self.proc_c.sendmail(self.proc_name,emessage,self.email_addr)
            self.ltime=mtime

#-----------------------------------
    def local_space(self):
        gbs=0
        cmd="du -s %s" % (self.trans_dir)
        s, o, e = self.proc_c.comm(cmd,shell=True,ignore_dry_run=True)
        if s == 0:
            mitems = o.split()
            gbs = BUFFERSIZE - (int(mitems[0]))/(1024*1024)
            self.proc_c.log("Local Space Avail: %s GBs" % (gbs),1)
            if gbs > MIN_BUFFER:
                return True
            self.proc_c.log("Insufficient space available",1)

        return False

#-----------------------------------
    def checkMarkerFile(self, afile, flist):
        for xfile in flist:
            if xfile == afile:
                return True
        return False

#-----------------------------------
    def getfiles(self, rdir):
        """ 
            get files from remote directory via guc --list 
        """
        cmd = "globus-url-copy -list %s" % "/".join([self.remote_url,rdir])
        self.proc_c.log(" Command:: '%s'" % (cmd), 4)
        s, o, e = self.proc_c.comm(cmd)
        if s == 0:
            flist = o.split()
            for afile in flist:
                self.proc_c.log("Next File is: '%s'" % (afile),1)
                if afile.endswith(self.ftype) and self.checkMarkerFile(".".join([afile,self.mtype]),flist):
                    yield afile  
                      
#-----------------------------------
    def getfiles_fromlist(self, rdir):
        """ 
            routine added as there was some inconsistencies when getting the dir listing via guc --list
            Too much disk access would cause the list to fail.  Here we can make the list is in someother way and 
            copy it from the remote destination.   Not sure we'll ever use it but it's a backup when needed
        """
        cmd = "globus-url-copy %s ./rawfilelist.txt" % "/".join([self.remote_url,self.remote_list])
        self.proc_c.log(" Command:: '%s'" % (cmd), 4)
        s, o, e = self.proc_c.comm(cmd)
        if s == 0:
            with open("rawfilelist.txt") as rflist:
                mylines = rflist.readlines()
                for aline in mylines:
                    afile = aline.rsplit('\n')
                    self.proc_c.log("Next file is: '%s'" % (afile),1)
                    if afile.endswith(self.ftype) and self.checkMarkerFile(".".join([afile,self.mtype]),flist):
                        yield afile
            os.remove("rawfilelist.txt")


#-----------------------------------
    def _calc_timeout(self, size):
        """ 
            Calculate the timeout to be used based on the size of the file to be transfered 
            and values of hard, rate and min timeout settings. 
        """

        # check for defaults
        if self.args.hard_timeout == HARD_TIMEOUT and \
           self.args.rate_timeout == RATE_TIMEOUT:
            return 0

        # hard timeout overrides rate timeout
        if self.args.hard_timeout != HARD_TIMEOUT:
            return self.args.hard_timeout

        # rate timeout is set, round up
        rate_timeout = math.ceil(float(size) /
                                 (self.args.rate_timeout * MEGABYTES))
        return max(MIN_TIMEOUT, rate_timeout) 

#------------------------
    def is_ready_to_transfer(self,fname):
        localfile="/".join([self.trans_status,fname]);
        if os.path.isfile(".".join([localfile,self.done])) or os.path.isfile(".".join([localfile,self.doing])):
            return False
        return True

#------------------------
    def _unixT(self):
        return int(time.mktime((datetime.now()).timetuple()))

#------------------------
    def manage_lock(self,fname,ltype,tally):
        """
            change lock files based on ltype input requested:
            if 'done' then rename in_progess file to done
            if 'doing' then create the in_progress file
            if 'failed' then remove the in_progress file
            catch errors
        """
        localfile="/".join([self.trans_status,fname])
        progressfile=".".join([localfile,self.doing])
        if ltype == self.done:
            donefile=".".join([localfile,self.done])
            try:
                os.rename(progressfile,donefile)
            except:
                tally['os_error']+=1
                self.proc_c.log("Copying file to done failed for %s" % (fname), 0)
                return False
            if self.args.copy_done_to_remote:
                remotedone="%s/%s/" % (self.remote_url,self.remote_dir)
                self.proc_c.log("prog=%s , done=%s, remotedone=%s" % (progressfile, donefile, remotedone), 1)
                r, e = self.copy_file(donefile, remotedone, self.timeout)
                if not r:
                    self.proc_c.log("Copy remote transfer done file failed %s" % (fname), 0)

        elif ltype == self.doing:
            cmd = "echo \"%s\" > %s" % (self._unixT(),progressfile)
            s, o, e = self.proc_c.comm(cmd, shell=True)
            if not s == 0:
                self.proc_c.log("Can't create new local lock file for '%s' " % (fname), 0)
                return False
        elif ltype == "failed":
            try:
                os.remove(progressfile)
            except:
                tally['os_error']+=1
                self.proc_c.log("Can't remove local file = '%s' " % (sfile), 0)
                return False
        return True

#------------------------
    def copy_file(self, src, dest, timeout):
        self.proc_c.log("copying: %s" % (src), 1)
        ret = False
        elapsed = 0
        ret, elapsed = self._call_guc(src, dest, timeout)
        return ret, elapsed

#------------------------
    def _call_guc(self, src, dest, timeout):
        """ helper function to copy_file to do just the
        globus-url-copy call."""

        guc_verbose = ""
        if self.args.verbosity >= 1:
            guc_verbose = "-vb"

        guc_cmd = "globus-url-copy -p %s %s %s %s" % \
                   (self.args.guc_parallel, guc_verbose, src, dest)

        self.proc_c.log("GUC : '%s'" % (guc_cmd),1)
        # call the copy command
        s, o, e = self.proc_c.comm(guc_cmd, self.timeout)
        if s != 0:
            self.proc_c.log("command failed: %s" % (guc_cmd), 0)
            self.proc_c.log("output: %s" % (o), 0)
            return False, e

        self.proc_c.log(o, 2)
        return True, e

#------------------------
    def validate_transfer(self,fname):
        mrkfile=".".join([fname,self.mtype])
        esize = 0
        for aline in open(mrkfile):
            finfo = aline.split(" ")
            try:
                esize = int(finfo[0])*1024
            except:
                self.proc_c.log("expected size not evaluated %s" & (finfo[0]), 0)
                return 2, esize
            diff_size = abs(int(os.stat(fname).st_size) - esize)
            self.proc_c.log("Size diff = %d" % (diff_size), 1)
            if diff_size/1024 >= 1:
                return 1, esize
        return 0, esize

#------------------------
    def transfer_file(self, tfile, tally):

        if not self.manage_lock(tfile,self.doing,tally):
            return False

        self.proc_c.log("Transfering File = %s" % (tfile), 0)
        remotefile="%s/%s/%s" % (self.remote_url, self.remote_dir, tfile)
        localfile="/".join([self.trans_dir,tfile])
        localgridfile ="/".join([self.local_url,localfile]) # --- the gridfile has the url for the local grid endpoint
        # --- start by copying the MRK file
        r, etime = self.copy_file(".".join([remotefile,self.mtype]),".".join([localgridfile,self.mtype]),timeout=0)
        v, esize = 0,0
        if not r:
            tally['copy_fail']+=1
            tally['mrk_fail']+=1
            self.proc_c.log("Transfer of marker failed for %s" % (tfile), 0)
            self.manage_lock(tfile,"failed",tally)
            try:
                os.remove(".".join([localfile,self.mtype]))
            except:
                self.proc_c.log("OS ERROR removing mrk file for %s" % (tfile),0)
                tally['os_error']+=1
            return False

        # --- now do target file
        r, etime = self.copy_file(remotefile,localgridfile,0)
        if r:
            v, esize = self.validate_transfer(localfile)
            tally['sum_size']+=esize
            tally['elapsed_time']+=etime
            if v == 0:
                tally['copy_succ']+=1
                self.manage_lock(tfile,self.done,tally)
                return True
            
        # --- rest are failed
        tally['copy_fail']+=1
        self.manage_lock(tfile,"failed",tally)

        self.proc_c.log("Transfer failed = %s" % (tfile), 0) 
        try:
            os.remove(localfile)
            os.remove(".".join([localfile,self.mtype]))
        except:
            self.proc_c.log("OS ERROR removing some file for %s" % (tfile),0)
            tally['os error']+=1

        if v == 2:
            tally['mrk_fail']+=1

        return False

#------------------------
    def go(self):
        """ The main application logic """

#        A tally for keeping count of various stats
        tally = dict(copy_tries=0, copy_succ = 0, copy_fail = 0, mrk_fail = 0, os_error = 0, 
                     sum_size=0.0, elapsed_time = 0.0)

        myloop = 0
        while True:
            icount = 0
            myloop += 1

            for key in tally:
                tally[key]=0
            self.notify()
            if self.held:
                self.proc_c.log("Found Hold Request, will sleep and check again",0)
                time.sleep(360)
                continue

            if not self.check_proxy():
                self.proc_c.log("No valid proxy at Time=%s" % datetime.now(),0)
                time.sleep(360)
                continue
            if(self.local_space()):
                for tfile in self.getfiles(self.remote_dir):
#                    if icount > 2:
#                        continue
#                    icount += 1
                    if self.is_ready_to_transfer(tfile):
                        tally['copy_tries']+=1
                        self.transfer_file(tfile,tally)
            self.proc_c.log("\n ================================================= \n",0)
            self.proc_c.log("Accumulated Results: loop # %d" % (myloop), 0)
            self.proc_c.log("attempts: %d" % (tally['copy_tries']),0)
            self.proc_c.log("success: %d" % (tally['copy_succ']),0)
            self.proc_c.log("copy_fail: %d" % (tally['copy_fail']),0)
            self.proc_c.log("mrk_fail: %d" % (tally['mrk_fail']),0)
            self.proc_c.log("OS Errors: %d" % (tally['os_error']),0)
            if tally['copy_succ'] == 0:
                size="--"
            else:
                size = "%f" % (tally['sum_size'] / tally['copy_succ'])
            self.proc_c.log("Ave size: %s Bytes" % (size), 0)
            if tally['elapsed_time'] == 0:
                et = "--"
            else:
                et = "%.3f" % ((tally['sum_size'] / tally['elapsed_time']) / (1 << 20))
            self.proc_c.log("Throughput:   %s MB/sec" % (et), 0)
            time.sleep(360)



def main():
    """ Generic program structure to parse args, initialize and start application """

    desc = """ Transfer pipeline tool """

    p = argparse.ArgumentParser(description=desc, epilog="None")

#----- main arguments for transfer targets, destination, and control
    p.add_argument("--remote-url",dest="remote_url",default=REMOTE_URL,help="gsiftp url of the remote endpoint")
    p.add_argument("--local-url",dest="local_url",default=LOCAL_URL,help="gsiftp url of the remote endpoint")
    p.add_argument("--remote-dir",dest="remote_dir",default=REMOTE_DIR,help="remote directory data is pulled from")
    p.add_argument("--remote-list",dest="remote_list",default=REMOTE_LIST,help="remote file list instead to guc --list")
    p.add_argument("--trans-dir",dest="trans_dir",default=TRANSFER_DIR,help="local directory to store data")
    p.add_argument("--trans-status",dest="trans_status",default=TRANSFER_STATUS_DIR,help="directory to store status of tranfers")
    p.add_argument("--file-type",dest="ftype",default=FTYPE,help="file extention of data files to be transfered")
    p.add_argument("--marker-type",dest="mtype",default=MRKTYPE,help="file extention of the marker file to be transfered")
    p.add_argument("--state-file",dest="state_file",default=STATE_FILE,help="file for external triggers: hold ")
    p.add_argument("--time-to-notify",dest="time_to_notify",default=TIME_TO_NOTIFY,help="how frequent to email notice")
    p.add_argument("--email-addr",dest="email_addr",default=EMAIL_ADDR,help="destination for email notices")

    p.add_argument("--copy-done",dest="copy_done_to_remote",action="store_true", default=False,help="Allows on to copy done file to the remote site")
    p.add_argument("--config-file",dest="config_file",default="None",help="override any configs via a json config file")


#------- arguments for debuging and others
    p.add_argument("--guc-parallel", dest="guc_parallel", default=GUC_PARALLEL, 
                    help="parallelism to use in globus-url-copy (-p arg) [%default]")
    p.add_argument("-n", "--dry-run", action="store_true", dest="dry_run", default=False,
                    help="display but don't run data movement commands")
    p.add_argument("-v", "--verbose", action="count", dest="verbosity", default=0,
                                 help="be verbose about actions, repeatable")
    p.add_argument("-q", "--quiet", action="store_true", dest="quiet", default=False,
                    help="be quiet, suppress normal and verbose output")
    p.add_argument("--hard-timeout", dest="hard_timeout",
                       default=HARD_TIMEOUT, help="the absolute timeout for "
                       "a single data transfer command, in seconds [%default]. "
                       "0 => no timeout, overrides --rate-timeout.")
    p.add_argument("--rate-timeout", dest="rate_timeout",
                       default=RATE_TIMEOUT, help="calculate timeout for data "
                       "transfer commands based on this transfer rate, in MB/s "
                       "[%default].  0 => no timeout, minimum calculated "
                       "timeout used will be " + str(MIN_TIMEOUT) + "secs")

    args = p.parse_args()

#-------- parse config file to override input and defaults
    val=vars(args)
    if not args.config_file == "None":
        try:
            with open(args.config_file) as config_file:
                configs=json.load(config_file)
            for key in configs:
                if key in val:
                    if isinstance(configs[key],unicode):
                        val[key]=configs[key].encode("ascii")
                    else:
                        val[key]=configs[key]
        except:
            p.error(" Could not open or parse the configfile ")
            return -1

    if args.quiet:
        args.verbosity = -1

    try:
        args.hard_timeout = int(args.hard_timeout)
        args.rate_timeout = int(args.rate_timeout)
    except ValueError:
        p.error("timeout value must be integers")
        
    try:
        tpl = transfer_pipeline(args)
        return(tpl.go())
    except (Exception), oops:
        if args.verbosity >= 2:
            import traceback
            traceback.print_exc()
        else:
            print oops
        return -1

if __name__ == "__main__":
    sys.exit(main())
