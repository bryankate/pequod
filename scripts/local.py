import localexperiments
import aggregate_json
from aggregate_json import aggregate_dir
import os
from os import system
import subprocess
from subprocess import Popen
import time
from time import sleep
from optparse import OptionParser
import json, fnmatch, re
import socket

parser = OptionParser()
parser.add_option("-b", "--backing", action="store", type="int", dest="nbacking", default=1)
parser.add_option("-c", "--caching", action="store", type="int", dest="ncaching", default=5)
parser.add_option("-p", "--startport", action="store", type="int", dest="startport", default=9000)
parser.add_option("-a", "--affinity", action="store_true", dest="affinity", default=False)
parser.add_option("-A", "--startcpu", action="store", type="int", dest="startcpu", default=0)
parser.add_option("-P", "--perfserver", action="store", type="int", dest="perfserver", default=-1)
parser.add_option("-f", "--part", action="store", type="string", dest="part", default=None)
parser.add_option("-g", "--clientgroups", action="store", type="int", dest="ngroups", default=1)
parser.add_option("-d", "--dumpdb", action="store_true", dest="dumpdb", default=False)
parser.add_option("-l", "--loaddb", action="store_true", dest="loaddb", default=False)
(options, args) = parser.parse_args()

nbacking = options.nbacking
ncaching = options.ncaching
startport = options.startport
affinity = options.affinity
startcpu = options.startcpu
perfserver = options.perfserver
ngroups = options.ngroups
dumpdb = options.dumpdb
loaddb = options.loaddb

nprocesses = nbacking + ncaching
hosts = []
pin = ""

topdir = None
uniquedir = None
hostpath = None
begintime = time.time()
localhost = socket.gethostname()

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
            hfile.write(localhost + "\t" + str(startport + h) + "\n");
        hfile.close()

    resdir = os.path.join(uniquedir, xname, ename)
    os.makedirs(resdir)
    return resdir

exps = localexperiments.exps

for x in exps:
    for e in x['defs']:
        # skip this experiment if it doesn't match an argument
        if args and not [argmatcher for argmatcher in args if fnmatch.fnmatch(e['name'], argmatcher)]:
            continue
        elif not args and e.get("disabled"):
            continue

        resdir = prepare_experiment(x["name"], e["name"])
        part = options.part if options.part else e['def_part']
        serverargs = " -H=" + hostpath + " -B=" + str(nbacking) + " -P=" + part

        print "Running experiment '" + e['name'] + "'."
        system("killall pqserver")
        system("killall postgres")
        sleep(3)

        dbhost = "127.0.0.1"
        dbport = 10000        
        dbenvpath = os.path.join(resdir, "store")
        os.makedirs(dbenvpath)
        
        dbhostpath = os.path.join(resdir, "dbhosts.txt")
        dbfile = open(dbhostpath, "w")
        dbprocs = []
        
        dbmonitor = False
        if 'def_db_writearound' in e:
            dbmonitor = e['def_db_writearound']

        for s in range(nprocesses):
            if s < nbacking:
                servercmd = e['backendcmd']
                
                if 'def_db_type' in e:
                    if e['def_db_type'] == 'berkeleydb':
                        servercmd = servercmd + \
                            " --berkeleydb --dbname=pequod_" + str(s) + \
                            " --dbenvpath=" + dbenvpath
                    elif e['def_db_type'] == 'postgres':
                        dbfile.write(dbhost + "\t" + str(dbport + s) + "\n");
                        servercmd = servercmd + \
                            " --postgres --dbname=pequod" + \
                            " --dbhost=" + dbhost + " --dbport=" + str(dbport + s)
                        
                        if dbmonitor:
                            servercmd = servercmd + " --monitordb"
                        
                        # start postgres server
                        fartfile = os.path.join(resdir, "fart_db_" + str(s) + ".txt")
                        dbpath = os.path.join(dbenvpath, "postgres_" + str(s))
                        os.makedirs(dbpath)
                        
                        cmd = "initdb " + dbpath + " -E utf8 -A trust" + \
                              " >> " + fartfile + " 2>> " + fartfile
                        print cmd
                        Popen(cmd, shell=True).wait()
                              
                        cmd = "postgres -h " + dbhost + " -p " + str(dbport + s) + \
                              " -D " + dbpath + " -c synchronous_commit=off -c fsync=off" + \
                              " >> " + fartfile + " 2>> " + fartfile
                        print cmd
                        dbprocs.append(Popen(cmd, shell=True))
                        sleep(2)
                        
                        cmd = "createdb -h " + dbhost + " -p " + str(dbport + s) + " pequod" + \
                              " >> " + fartfile + " 2>> " + fartfile
                        print cmd
                        Popen(cmd, shell=True).wait()
            else:
                servercmd = e['cachecmd']
                
            outfile = os.path.join(resdir, "output_srv_")
            fartfile = os.path.join(resdir, "fart_srv_")
  
            if affinity:
                pin = "numactl -C " + str(startcpu + s) + " "
                
            if s == perfserver:
                perf = "perf record -g -o " + os.path.join(resdir, "perf-") + str(s) + ".dat "
            else:
                perf = ""

            full_cmd = pin + perf + servercmd + serverargs + \
                " -kl=" + str(startport + s) + \
                " > " + outfile + str(s) + ".txt" + \
                " 2> " + fartfile + str(s) + ".txt &"

            print full_cmd
            system(full_cmd)
            
        dbfile.close()
        sleep(3)

        if loaddb and dbmonitor and e['def_db_type'] == 'postgres':
            print "Populating backend from database archive."
            dbarchive = os.path.join("dumps", x["name"], e["name"], "b_" + str(nbacking))
            procs = []
            for s in range(nbacking):
                archfile = os.path.join(dbarchive, "pequod_" + str(s))
                cmd = "pg_restore -p " + str(dbport + s) + " -d pequod -a " + archfile
                print cmd
                procs.append(Popen(cmd, shell=True))
                
            for p in procs:
                p.wait()
        elif 'populatecmd' in e:
            print "Populating backend."
            procs = []
            popcmd = e['populatecmd'] + " -H=" + hostpath + " -B=" + str(nbacking)
            fartfile = os.path.join(resdir, "fart_pop.txt")
            
            if dbmonitor:
                popcmd = popcmd + " --writearound --dbhostfile=" + dbhostpath
            
            if affinity:
                pin = "numactl -C " + str(startcpu + nprocesses) + " "
            
            full_cmd = pin + popcmd + " 2> " + fartfile

            print full_cmd
            procs.append(Popen(full_cmd, shell=True))

            for p in procs:
                p.wait()

        if dumpdb and dbmonitor and e['def_db_type'] == 'postgres':
            print "Dumping backend database after population."
            procs = []
            dbarchive = os.path.join("dumps", x["name"], e["name"], "b_" + str(nbacking))
            for s in range(nbacking):
                archfile = os.path.join(dbarchive, "pequod_" + str(s))
                system("rm -rf " + archfile + "; mkdir -p " + dbarchive)
                cmd = "pg_dump -p " + str(dbport + s) + " -f " + archfile + " -a -F d pequod" 
                print cmd
                procs.append(Popen(cmd, shell=True))
                
            for p in procs:
                p.wait()

        print "Starting app clients."
        procs = []
        clientcmd = e['clientcmd'] + " -H=" + hostpath + " -B=" + str(nbacking)
        
        for c in range(ngroups):
            outfile = os.path.join(resdir, "output_app_")
            fartfile = os.path.join(resdir, "fart_app_")
            
            if dbmonitor:
                clientcmd = clientcmd + " --writearound --dbhostfile=" + dbhostpath
                
            if affinity:
                pin = "numactl -C " + str(startcpu + nprocesses + c) + " "

            full_cmd = pin + clientcmd + \
                " --ngroups=" + str(ngroups) + " --groupid=" + str(c) + \
                " > " + outfile + str(c) + ".json" + \
                " 2> " + fartfile + str(c) + ".txt"

            print full_cmd
            procs.append(Popen(full_cmd, shell=True));
            
        # wait for clients to finish
        for p in procs:
            p.wait()
    
        if ngroups > 1:
            aggregate_dir(resdir)
    
        print "Done experiment. Results are stored at", resdir
        system("killall pqserver")
        system("killall postgres")
