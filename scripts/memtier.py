#!/usr/bin/env python

import sys
import os
from os import system
import signal
import subprocess
from subprocess import Popen
import shutil
import time
from time import sleep
from optparse import OptionParser
import json, fnmatch, re
import socket
import math

parser = OptionParser()
parser.add_option("-e", "--expfile", action="store", type="string", dest="expfile", 
                  default=os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), "exp", "memtierexperiments.py"))
parser.add_option("-L", "--link", action="store", type="string", dest="symlink", default=None)
parser.add_option("-K", "--killall", action="store_true", dest="killall", default=False)
parser.add_option("-p", "--port", action="store", type="int", dest="port", default=9000)
parser.add_option("-t", "--threads", action="store", type="int", dest="threads", default=4)
parser.add_option("-c", "--clients", action="store", type="int", dest="clients", default=50)
parser.add_option("-d", "--duration", action="store", type="int", dest="duration", default=30)
parser.add_option("-x", "--prefix", action="store", type="string", dest="prefix", default="m|")
parser.add_option("-s", "--valsize", action="store", type="string", dest="valsize", default=256)
parser.add_option("-a", "--affinity", action="store_true", dest="affinity", default=False)
parser.add_option("-A", "--startcpu", action="store", type="int", dest="startcpu", default=0)
parser.add_option("-P", "--perfserver", action="store_true", dest="perfserver", default=False)
(options, args) = parser.parse_args()

maxcpus = 24
expfile = options.expfile
symlink = options.symlink
killall = options.killall
port = options.port
threads = options.threads
clients = options.clients
duration = options.duration
prefix = options.prefix
valsize = options.valsize
affinity = options.affinity
startcpu = options.startcpu
perfserver = options.perfserver

pin = ""

topdir = None
uniquedir = None
hostpath = None
begintime = time.time()

def prepare_experiment(xname, ename):
    global topdir, uniquedir, hostpath
    if topdir is None:
        topdir = "results"

    if uniquedir is None:
        expdir = "exp_" + time.strftime("%Y_%m_%d-%H_%M_%S")
        uniquedir = os.path.join(topdir, expdir)
        os.makedirs(uniquedir)

        if os.path.lexists("last"):
            os.unlink("last")
        os.symlink(uniquedir, "last")

        if symlink:
            linkpath = os.path.join(topdir, symlink)
            if os.path.lexists(linkpath):
                os.unlink(linkpath)
            os.symlink(expdir, linkpath)

    resdir = os.path.join(uniquedir, xname, ename if ename else "")
    os.makedirs(resdir)
    return (os.path.join(uniquedir, xname), resdir)

def kill_proc(p):
    (proc, outfd, errfd) = p
    proc.kill()
    if outfd:
        outfd.close()
    if errfd:
        errfd.close()

def wait_for_proc(p):
    (proc, outfd, errfd) = p
    proc.wait()
    if outfd:
        outfd.close()
    if errfd:
        errfd.close()

def run_cmd_bg(cmd, outfile=None, errfile=None, sh=False):
    global logfd
    
    if outfile:
        outfd = open(outfile, "a")
    else:
        outfd = None
    if errfile:
        if errfile == outfile:
            errfd = outfd
        else:
            errfd = open(errfile, "a")
    else:
        errfd = None
    
    print cmd
    logfd.write(cmd + "\n")
    return (Popen(cmd.split(), stdout=outfd, stderr=errfd, shell=sh), outfd, errfd)

def run_cmd(cmd, outfile=None, errfile=None, sh=False):
    wait_for_proc(run_cmd_bg(cmd, outfile, errfile, sh))

def start_redis(expdef):
    fartfile = os.path.join(resdir, "fart_redis.txt")
    datadir = os.path.join(dbenvpath, "redis")
    os.makedirs(datadir)
    
    conf = "dir " + datadir + "\n" + \
           "port " + str(port) + "\n" + \
           "pidfile " + os.path.join(dbenvpath, "redis.pid." + str(port)) + "\n" + \
           "include " + os.path.join(os.path.dirname(os.path.realpath(__file__)), "exp", "redis.conf")
           
    confpath = os.path.join(dbenvpath, "redis.conf")
    conffile = open(confpath, "w")
    conffile.write(conf)
    conffile.close()
    
    if affinity:
        pin = "numactl -C " + str(startcpu) + " "
    else:
        pin = ""
    
    cmd = pin + "redis-server " + confpath
    return run_cmd_bg(cmd, fartfile, fartfile)

def start_memcache(expdef):
    fartfile = os.path.join(resdir, "fart_memcache.txt")
    
    if affinity:
        pin = "numactl -C " + str(startcpu) + " "
    else:
        pin = ""

    if expdef.get('def_memcache_args'):
        args = expdef.get('def_memcache_args') + " "
    else:
        args = ""

    cmd = pin + "memcached " + args + "-t 1 -p " + str(port)
    return run_cmd_bg(cmd, fartfile, fartfile)

# load experiment definitions as global 'exps'
exph = open(expfile, "r")
exec(exph, globals())
exph.close()

for x in exps:
    expdir = None
    
    for e in x['defs']:
        expname = e['name'] if 'name' in e else None
        
        # skip this experiment if it doesn't match an argument
        if args and not [argmatcher for argmatcher in args if fnmatch.fnmatch(x['name'], argmatcher)]:
            continue
        elif not args and e.get("disabled"):
            continue

        if killall:
            Popen("killall pqserver memcached redis-server", shell=True).wait()

        print "Running experiment" + ((" '" + expname + "'") if expname else "") + \
              " in test '" + x['name'] + "'."
        (expdir, resdir) = prepare_experiment(x["name"], expname)
        logfd = open(os.path.join(resdir, "cmd_log.txt"), "w")
        
        rediscompare = e.get('def_redis_compare')
        memcachecompare = e.get('def_memcache_compare')
        serverprocs = [] 
        
        if rediscompare:
            dbenvpath = os.path.join(resdir, "store")
            os.makedirs(dbenvpath)
            serverprocs.append(start_redis(e))
                
        elif memcachecompare:
            serverprocs.append(start_memcache(e))
                
        else:
            servercmd = e['cachecmd']
            fartfile = os.path.join(resdir, "fart_srv.txt")
  
            if affinity:
                pin = "numactl -C " + str(startcpu) + " "
                
            if perfserver:
                perf = "perf record -g -o " + os.path.join(resdir, "perf-srv") + ".dat "
            else:
                perf = ""

            full_cmd = pin + perf + servercmd + " -kl=" + str(port)
            serverprocs.append(run_cmd_bg(full_cmd, fartfile, fartfile))
                
        sleep(3)
        
        if 'populatecmd' in e:
            popcmd = e['populatecmd']
            fartfile = os.path.join(resdir, "fart_pop.txt")
                    
            if affinity:
                pin = "numactl -C " + str(startcpu + 1) + " "
                        
            full_cmd = pin + popcmd + " -p " + str(port) + \
                       " --prefix=" + prefix + " --valsize=" + str(valsize)
            run_cmd(full_cmd, fartfile, fartfile);
            
        clientcpulist = ""
        if startcpu + threads + 1 > maxcpus:
            # if we want to run more clients than we have processors left, 
            # just run them all on the set of remaining cpus
            clientcpulist = ",".join([str(startcpu + c) for c in range(maxcpus - (startcpu))])
                    
        outfile = os.path.join(resdir, "output_bench.txt")
        fartfile = os.path.join(resdir, "fart_bench.txt")
        clientcmd = e['clientcmd']
        
        if affinity:
            pin = "numactl -C " + (clientcpulist if clientcpulist else str(startcpu + 1) + "-" + str(startcpu + threads + 1)) + " "

        full_cmd = pin + clientcmd + " -p " + str(port) + \
                   " -t " + str(threads) + " -c " + str(clients) + \
                   " --test-time=" + str(duration) + " --key-prefix=" + prefix + \
                   " --data-size=" + str(valsize)
        
        run_cmd(full_cmd, outfile);
            
        for p in serverprocs:
            kill_proc(p)
        
        if killall:
            Popen("killall pqserver memcached redis-server", shell=True).wait()
        
        logfd.close()
        print "Done experiment. Results are stored at", resdir
        
        port += 1
