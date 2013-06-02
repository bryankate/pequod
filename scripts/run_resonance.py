#!/usr/bin/env python

import localexperiments
import os
from os import system
#import subprocess
from subprocess import Popen, call
import time
from time import sleep
from optparse import OptionParser
import json, fnmatch, re
import socket
import stat

parser = OptionParser()
parser.add_option("-b", "--backing", action="store", type="int", dest="nbacking", default=1)
parser.add_option("-c", "--caching", action="store", type="int", dest="ncaching", default=5)
parser.add_option("-C", "--clients", action="store", type="int", dest="nclients", default=5)
parser.add_option("-p", "--startport", action="store", type="int", dest="startport", default=9000)
parser.add_option("-a", "--affinity", action="store_true", dest="affinity", default=False)
parser.add_option("-A", "--startcpu", action="store", type="int", dest="startcpu", default=0)
parser.add_option("-P", "--perfserver", action="store", type="int", dest="perfserver", default=-1)
parser.add_option("-f", "--part", action="store", type="string", dest="part", default=None)
(options, args) = parser.parse_args()

nbacking = options.nbacking
ncaching = options.ncaching
nclients = options.ncaching
startport = options.startport
affinity = options.affinity
startcpu = options.startcpu
perfserver = options.perfserver
count = 0

nprocesses = nbacking + ncaching
hosts = []
pin = ""

topdir = None
uniquedir = None
hostpath = None
begintime = time.time()

def prepare_experiment(xname, ename):
    global topdir, uniquedir, hostpath, begintime, nprocesses
    if topdir is None:
        topdir = "results"

    if uniquedir is None:
        uniquedir = topdir + "/exp_" + str(begintime)
        os.makedirs(uniquedir)

        if os.path.lexists("last"):
            os.unlink("last")
        os.symlink(uniquedir, "last")

        hostpath = os.path.join(uniquedir, "hosts.txt")
        hfile = open(hostpath, "w")
        for h in range(nprocesses):
            hfile.write('cuda-1-' + str(h+1) + "\t" + str(startport) + "\n");
        hfile.close()

    resdir = os.path.join(uniquedir, xname, ename)
    os.makedirs(resdir)
    return resdir

exps = localexperiments.exps
homedir = os.path.expanduser("~")

for x in exps:
    for e in x['defs']:
        # skip this experiment if it doesn't match an argument
        if args and not [argmatcher for argmatcher in args if fnmatch.fnmatch(e['name'], argmatcher)]:
            continue
        elif not args and e.get("disabled"):
            continue

        resdir = prepare_experiment(x["name"], e["name"])
        part = options.part if options.part else e['def_part']
        servercmd = e['servercmd'] + " -H=" + hostpath + " -B=" + str(nbacking) + " -P=" + part

        print "Running experiment '" + e['name'] + "'."
        
        for s in range(nprocesses):
            outfile = os.path.join(resdir, "output_srv_")
            fartfile = os.path.join(resdir, "fart_srv_")
            
            if affinity:
                pin = "numactl -C " + str(startcpu + s) + " "
                
            if s == perfserver:
                perf = "perf record -g -o " + os.path.join(resdir, "perf-") + str(s) + ".dat "
            else:
                perf = ""

            full_cmd = pin + perf + servercmd + \
                " -kl=" + str(startport) + \
                " > " + outfile + str(s) + ".txt" + \
                " 2> " + fartfile + str(s) + ".txt &"
            sname = homedir + "/start_pequod_srv" + str(s) + ".sh"
            srvfile = open(sname, "w")
            srvfile.write("#!/bin/bash\n\n")
            srvfile.write("cd ~/pequod\n")
            srvfile.write(". add_exports.sh\n\n")
            srvfile.write(full_cmd)
            srvfile.close()
            st = os.stat(sname)
            os.chmod(sname, st.st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH )
            ssh_cmd = 'ssh cuda-1-%d "./start_pequod_srv%d.sh"' % (s+1, s)
            call([ssh_cmd], shell=True)

        sleep(3)

        print "Starting app clients."
        procs = []
        clientcmd = e['clientcmd'] + " -H=" + hostpath + " -B=" + str(nbacking)
        print clientcmd
        
        for c in range(1):
            outfile = os.path.join(resdir, "output_app_")
            fartfile = os.path.join(resdir, "fart_app_")
            
            if affinity:
                pin = "numactl --physcpubind=" + str(startcpu + nprocesses + c) + " "

            full_cmd = pin + clientcmd + \
                " > " + outfile + str(c) + ".json" + \
                " 2> " + fartfile + str(c) + ".txt"

            print full_cmd
            procs.append(Popen(full_cmd, shell=True));
            
        # wait for clients to finish
        for p in procs:
            p.wait()
    
        print "Cleaning up..."
        call(["rm -f " + homedir + "/start_pequod_srv*"], shell=True)
        for s in range(nprocesses):
            ssh_cmd = 'ssh cuda-1-%d "killall pqserver"' % (s+1)
            call([ssh_cmd], shell=True)

        print "Experiment complete. Results are stored at", resdir
