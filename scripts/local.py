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
import lib.aggregate
from lib.aggregate import aggregate_dir
import lib.gnuplotter
from lib.gnuplotter import make_gnuplot


parser = OptionParser()
parser.add_option("-e", "--expfile", action="store", type="string", dest="expfile", 
                  default=os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), "exp", "testexperiments.py"))
parser.add_option("-L", "--link", action="store", type="string", dest="symlink", default=None)
parser.add_option("-K", "--killall", action="store_true", dest="killall", default=False)
parser.add_option("-b", "--backing", action="store", type="int", dest="nbacking", default=1)
parser.add_option("-c", "--caching", action="store", type="int", dest="ncaching", default=5)
parser.add_option("-p", "--startport", action="store", type="int", dest="startport", default=7000)
parser.add_option("-M", "--moveports", action="store_true", dest="moveports", default=False)
parser.add_option("-a", "--affinity", action="store_true", dest="affinity", default=False)
parser.add_option("-A", "--startcpu", action="store", type="int", dest="startcpu", default=0)
parser.add_option("-P", "--perfserver", action="store", type="int", dest="perfserver", default=-1)
parser.add_option("-f", "--part", action="store", type="string", dest="part", default=None)
parser.add_option("-g", "--clientgroups", action="store", type="int", dest="ngroups", default=1)
parser.add_option("-D", "--dbstartport", action="store", type="int", dest="dbstartport", default=10000)
parser.add_option("-d", "--dumpdb", action="store_true", dest="dumpdb", default=False)
parser.add_option("-l", "--loaddb", action="store_true", dest="loaddb", default=False)
parser.add_option("-r", "--ramfs", action="store", type="string", dest="ramfs", default="/mnt/tmp")
(options, args) = parser.parse_args()

maxcpus = 24
expfile = options.expfile
symlink = options.symlink
killall = options.killall
nbacking = options.nbacking
ncaching = options.ncaching
startport = options.startport
moveports = options.moveports
affinity = options.affinity
startcpu = options.startcpu
perfserver = options.perfserver
ngroups = options.ngroups
dbstartport = options.dbstartport
dumpdb = options.dumpdb
loaddb = options.loaddb
ramfs = options.ramfs

nprocesses = nbacking + ncaching
ndbs = nbacking if nbacking > 0 else ncaching
dbhost = "127.0.0.1"
hosts = []
pin = ""

topdir = None
uniquedir = None
hostpath = None
begintime = time.time()
localhost = socket.gethostname()

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

    if moveports or not hostpath:
        hostpath = os.path.join(uniquedir, "hosts.txt")
        hfile = open(hostpath, "w")
        for h in range(nprocesses):
            hfile.write(localhost + "\t" + str(startport + h) + "\n");
        hfile.close()
        
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

def check_database_env(expdef):
    global dbenvpath
    
    # possibly redirect to ramfs
    if expdef.get('def_db_in_memory'):
        cmd = "mount | grep '" + ramfs + "' | grep -o '[0-9]\\+[kmg]'"
        (a, _) = Popen(cmd, stdout=subprocess.PIPE, shell=True).communicate()
        if a:
            if len(a) > 8:
                print "[[37;1minfo[0m] Memory FS is %s. (~%sGb)" % (a[:-1],a[:-8])
            else:
                print "[[33;1mwarn[0m] Memory FS is small, %s is less than 1Gb memory." % (a[:-1])
            (a,_) = Popen("df " + ramfs + " | grep -o '[0-9]\+%'", stdout=subprocess.PIPE, shell=True).communicate()
            if a[0] is not "0":
                print "[[33;1mwarn[0m] " + ramfs + " is %s full." % (a[:-1])
        else:
            print "[[31mFAIL[0m] '" + ramfs + "' not found or missing size."
            print "[[31mFAIL[0m] search command:\"%s\"." % (cmd)
            exit()
        dbenvpath = os.path.join(ramfs, dbenvpath)

def start_postgres(expdef, id, ncpus):
    fartfile = os.path.join(resdir, "fart_db_" + str(id) + ".txt")
    dbpath = os.path.join(dbenvpath, "postgres_" + str(id))
    os.makedirs(dbpath)

    cmd = "initdb " + dbpath + " -E utf8 -A trust"
    run_cmd(cmd, fartfile, fartfile)

    if affinity:
        pin = "numactl -C " + ",".join([str(id + c) for c in range(ncpus)]) + " "
    else:
        pin = ""
    
    if expdef.get('def_db_flags'):
        flags = expdef.get('def_db_flags')
    else:
        flags = ""
    
    cmd = pin + "postgres -h " + dbhost + " -p " + str(dbstartport + id) + \
          " -D " + dbpath + " " + flags
    proc = run_cmd_bg(cmd, fartfile, fartfile)
    sleep(2)
    
    cmd = "createdb -h " + dbhost + " -p " + str(dbstartport + id) + " pequod"
    run_cmd(cmd, fartfile, fartfile)

    if 'def_db_sql_script' in expdef:
        cmd = "psql -p %d pequod -f %s" % (dbstartport + id, expdef['def_db_sql_script'])
        run_cmd(cmd, fartfile, fartfile)
        
    if 'def_db_s_import' in expdef:
        cmd = "pg_restore -a -p %d -d pequod -Fc %s" % (dbstartport + id, expdef['def_db_s_import'])
        run_cmd(cmd, fartfile, fartfile)
        
    return proc

def start_redis(expdef, id):
    fartfile = os.path.join(resdir, "fart_redis_" + str(id) + ".txt")
    datadir = os.path.join(dbenvpath, "redis_" + str(id))
    os.makedirs(datadir)
    
    conf = "dir " + datadir + "\n" + \
           "port " + str(startport + id) + "\n" + \
           "pidfile " + os.path.join(dbenvpath, "redis.pid." + str(id)) + "\n" + \
           "include " + os.path.join(os.path.dirname(os.path.realpath(__file__)), "exp", "redis.conf")
           
    confpath = os.path.join(dbenvpath, "redis_" + str(id) + ".conf")
    conffile = open(confpath, "w")
    conffile.write(conf)
    conffile.close()
    
    if affinity:
        pin = "numactl -C " + str(startcpu + id) + " "
    else:
        pin = ""
    
    cmd = pin + "redis-server " + confpath
    return run_cmd_bg(cmd, fartfile, fartfile)

def start_memcache(expdef, id):
    fartfile = os.path.join(resdir, "fart_memcache_" + str(id) + ".txt")
    
    if affinity:
        pin = "numactl -C " + str(startcpu + id) + " "
    else:
        pin = ""

    if expdef.get('def_memcache_args'):
        args = expdef.get('def_memcache_args') + " "
    else:
        args = ""

    cmd = pin + "memcached " + args + "-t 1 -p " + str(startport + id)
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
            Popen("killall pqserver postgres memcached redis-server", shell=True).wait()

        print "Running experiment" + ((" '" + expname + "'") if expname else "") + \
              " in test '" + x['name'] + "'."
        (expdir, resdir) = prepare_experiment(x["name"], expname)
        logfd = open(os.path.join(resdir, "cmd_log.txt"), "w")
        
        if 'def_build' in e:
            fartfile = os.path.join(resdir, "fart_build.txt")
            run_cmd(e['def_build'], fartfile, fartfile, sh=True)
             
        usedb = True if 'def_db_type' in e else False
        rediscompare = e.get('def_redis_compare')
        memcachecompare = e.get('def_memcache_compare')
        dbcompare = e.get('def_db_compare')
        dbmonitor = e.get('def_db_writearound')
        serverprocs = [] 
        dbprocs = []
        
        if usedb or rediscompare:
            dbenvpath = os.path.join(resdir, "store")
            check_database_env(e)
            os.makedirs(dbenvpath)
        
        if dbcompare:
            # if we are comparing to a db, don't start any pequod servers.
            # the number of caching servers (-c) will be used as the number 
            # of simultaneous connections to the database.
            if ncaching < 1:
                print "ERROR: -c must be > 0 for DB comparison experiments"
                exit(-1)
                
            dbprocs.append(start_postgres(e, 0, ncaching))
            
        elif rediscompare:
            if ncaching < 1:
                print "ERROR: -c must be > 0 for redis comparison experiments"
                exit(-1)
                
            for s in range(ncaching):
                dbprocs.append(start_redis(e, s))
                
        elif memcachecompare:
            if ncaching < 1:
                print "ERROR: -c must be > 0 for memcache comparison experiments"
                exit(-1)
                
            for s in range(ncaching):
                dbprocs.append(start_memcache(e, s))
                
        else:
            if usedb:
                dbhostpath = os.path.join(resdir, "dbhosts.txt")
                dbfile = open(dbhostpath, "w")
                
            for s in range(nprocesses):
                if s < nbacking:
                    servercmd = e['backendcmd']
                else:
                    servercmd = e['cachecmd']
                
                if usedb and s < ndbs:
                    if e['def_db_type'] == 'berkeleydb':
                        servercmd = servercmd + \
                            " --berkeleydb --dbname=pequod_" + str(s) + \
                            " --dbenvpath=" + dbenvpath
                    elif e['def_db_type'] == 'postgres':
                        servercmd = servercmd + \
                            " --postgres --dbname=pequod" + \
                            " --dbhost=" + dbhost + " --dbport=" + str(dbstartport + s)
                        if dbmonitor:
                            servercmd = servercmd + " --monitordb"
    
                        dbfile.write(dbhost + "\t" + str(dbstartport + s) + "\n");
                        dbprocs.append(start_postgres(e, s, 1))
    
                part = options.part if options.part else e['def_part']
                serverargs = " -H=" + hostpath + " -B=" + str(nbacking) + " -P=" + part
                fartfile = os.path.join(resdir, "fart_srv_" + str(s) + ".txt")
      
                if affinity:
                    pin = "numactl -C " + str(startcpu + s) + " "
                    
                if s == perfserver:
                    perf = "perf record -g -o " + os.path.join(resdir, "perf-") + str(s) + ".dat "
                else:
                    perf = ""
    
                full_cmd = pin + perf + servercmd + serverargs + " -kl=" + str(startport + s)
                serverprocs.append(run_cmd_bg(full_cmd, fartfile, fartfile))
        
            if usedb:
                dbfile.close()
                
        sleep(3)

        clientcpulist = ""
        if startcpu + nprocesses + ngroups > maxcpus:
            # if we want to run more clients than we have processors left, 
            # just run them all on the set of remaining cpus
            clientcpulist = ",".join([str(startcpu + nprocesses + c) for c in range(maxcpus - (startcpu + nprocesses))])

        if 'initcmd' in e:
            print "Initializing cache servers."
            initcmd = e['initcmd']
            fartfile = os.path.join(resdir, "fart_init.txt")
            
            if dbcompare:
                initcmd = initcmd + " --dbport=%d --dbpool-max=%d" % (dbstartport, ncaching)
            else:
                initcmd = initcmd + " -H=" + hostpath + " -B=" + str(nbacking)
            
            if affinity:
                pin = "numactl -C " + str(startcpu + nprocesses) + " "
            
            full_cmd = pin + initcmd
            run_cmd(full_cmd, fartfile, fartfile)

        if loaddb and dbmonitor and e['def_db_type'] == 'postgres':
            print "Populating backend from database archive."
            dbarchive = os.path.join("dumps", x["name"], e["name"], "nshard_" + str(ndbs))
            procs = []
            for s in range(ndbs):
                archfile = os.path.join(dbarchive, "pequod_" + str(s))
                cmd = "pg_restore -p " + str(dbport + s) + " -d pequod -a " + archfile
                procs.append(run_cmd_bg(cmd))
                
            for p in procs:
                wait_for_proc(p)
                
        elif 'populatecmd' in e:
            print "Populating backend."
            popcmd = e['populatecmd']
            npop = 1 if e.get('def_single_pop') else ngroups
                    
            if dbcompare:
                if npop >= ncaching:
                    pool = 1;
                else:
                    pool = math.ceil(ncaching / npop)
                    
                popcmd = popcmd + " --dbport=%d --dbpool-max=%d" % (dbstartport, pool)
            else:
                popcmd = popcmd + " -H=" + hostpath + " -B=" + str(nbacking)
            
            if dbmonitor:
                popcmd = popcmd + " --writearound --dbhostfile=" + dbhostpath
          
            popprocs = []
            for c in range(npop):
                fartfile = os.path.join(resdir, "fart_pop_" + str(c) + ".txt")
                            
                if affinity:
                    pin = "numactl -C " + (clientcpulist if clientcpulist and npop > 1 \
                                                         else str(startcpu + nprocesses + c)) + " "
                            
                full_cmd = pin + popcmd + \
                    " --ngroups=" + str(npop) + " --groupid=" + str(c)

                popprocs.append(run_cmd_bg(full_cmd, fartfile, fartfile));
            
            for p in popprocs:
                wait_for_proc(p)
            
        if dumpdb and dbmonitor and e['def_db_type'] == 'postgres':
            print "Dumping backend database after population."
            procs = []
            dbarchive = os.path.join("dumps", x["name"], e["name"], "nshard_" + str(ndbs))
            for s in range(ndbs):
                archfile = os.path.join(dbarchive, "pequod_" + str(s))
                system("rm -rf " + archfile + "; mkdir -p " + dbarchive)
                cmd = "pg_dump -p " + str(dbport + s) + " -f " + archfile + " -a -F d pequod" 
                procs.append(run_cmd(cmd))
                
            for p in procs:
                wait_for_proc(p)


        print "Starting app clients."
        clientprocs = []
                    
        for c in range(ngroups):
            outfile = os.path.join(resdir, "output_app_" + str(c) + ".json")
            fartfile = os.path.join(resdir, "fart_app_" + str(c) + ".txt")
            clientcmd = e['clientcmd']
            
            if dbcompare:
                if ngroups >= ncaching:
                    pool = 1;
                else:
                    pool = math.ceil(ncaching / ngroups)
                    
                clientcmd = clientcmd + " --dbport=%d --dbpool-max=%d" % \
                            (dbstartport, pool)
            else:
                clientcmd = clientcmd + " -H=" + hostpath + " -B=" + str(nbacking)
            
            if dbmonitor:
                clientcmd = clientcmd + " --writearound --dbhostfile=" + dbhostpath
            
            if affinity:
                pin = "numactl -C " + (clientcpulist if clientcpulist else str(startcpu + nprocesses + c)) + " "

            full_cmd = pin + clientcmd + \
                       " --ngroups=" + str(ngroups) + " --groupid=" + str(c)

            clientprocs.append(run_cmd_bg(full_cmd, outfile, fartfile));
            
        # wait for clients to finish
        for p in clientprocs:
            wait_for_proc(p)
    
        for p in serverprocs + dbprocs:
            kill_proc(p)
        
        if killall:
            Popen("killall pqserver postgres memcached redis-server", shell=True).wait()
        
        if moveports:
            startport += 100
            dbstartport += 100
        
        if ngroups > 1:
            aggregate_dir(resdir, x['name'])
    
        if usedb and e.get('def_db_in_memory'):
           shutil.rmtree(dbenvpath)

        logfd.close()
        print "Done experiment. Results are stored at", resdir
    
    if expdir and 'plot' in x:
        make_gnuplot(x['name'], expdir, x['plot'])
        