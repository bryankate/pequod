#!/usr/bin/env python

import sys
import os
from os import system
import subprocess
import time
from time import sleep
from optparse import OptionParser
import json, fnmatch, re
import socket
import getpass
import lib.aggregate
from lib.aggregate import aggregate_dir
import lib.gnuplotter
from lib.gnuplotter import make_gnuplot
import lib.ec2 as ec2

parser = OptionParser()
parser.add_option("-e", "--expfile", action="store", type="string", dest="expfile", 
                  default=os.path.join(os.path.dirname(os.path.realpath(sys.argv[0])), "exp", "testexperiments.py"))
parser.add_option("-b", "--backing", action="store", type="int", dest="nbacking", default=1)
parser.add_option("-c", "--caching", action="store", type="int", dest="ncaching", default=5)
parser.add_option("-f", "--part", action="store", type="string", dest="part", default=None)
parser.add_option("-g", "--clientgroups", action="store", type="int", dest="ngroups", default=1)
parser.add_option("-t", "--terminate", action="store_true", dest="terminate", default=False)
parser.add_option("-u", "--user", action="store", type="string", dest="user", default=getpass.getuser())
(options, args) = parser.parse_args()

expfile = options.expfile
nbacking = options.nbacking
ncaching = options.ncaching
ngroups = options.ngroups
terminate = options.terminate
user = options.user
port = 9000

nservers = nbacking + ncaching
nmachines = nservers + ngroups
topdir = None
uniquedir = None
hostpath = None

# startup machines on EC2
ec2.connect()

running = []
newmachines = []

def prepare_instances(num, type, tag):
    global running, newmachines
    
    running_ = ec2.get_running_instances(type, tag)
    pending_ = []
    
    if len(running_) < num:
        nstarted = len(running_)
        
        stopped_ = ec2.get_stopped_instances(type)
        if stopped_:
            ntostart = min(len(stopped_), num - nstarted)
            ec2.startup_machines(stopped_[0:ntostart])
            pending_ += stopped_[0:ntostart]
            nstarted += ntostart
        
        if nstarted < num:
            checkedout = ec2.checkout_machines(num - nstarted, type)
            pending_ += checkedout
            newmachines += checkedout
    
    while (pending_):
        print "Waiting for machines to start..."
        again = []
        
        for p in pending_:
            if ec2.is_running(p):
                p.add_tag(tag[0], tag[1])
            else:
                again.append(p)
        
        pending_ = again
        sleep(5)
        
    hosts_ = []
    running_ = ec2.get_running_instances(type, tag)
    running += running_
    
    for r in running_:
        r.update(True)
        hosts_.append({'dns': r.public_dns_name, 'ip': r.private_ip_address})
    return hosts_

def prepare_experiment(xname, ename):
    global topdir, uniquedir, hostpath, hosts, running, nprocesses
    if topdir is None:
        topdir = "results"

    if uniquedir is None:
        uniquedir = topdir + "/exp_" + time.strftime("%Y_%m_%d-%H_%M_%S")
        os.makedirs(uniquedir)

        if os.path.lexists("last"):
            os.unlink("last")
        os.symlink(uniquedir, "last")

        hostpath = os.path.join(uniquedir, "hosts.txt")
        hfile = open(hostpath, "w")
        
        for s in backinghosts + cachehosts:
            hfile.write(s['ip'] + "\t" + str(port) + "\n");
        hfile.close()

    resdir = os.path.join(uniquedir, xname, ename if ename else "")
    os.makedirs(resdir)
    return (os.path.join(uniquedir, xname), resdir)

# load experiment definitions as global 'exps'
exph = open(expfile, "r")
exec(exph, globals())
exph.close()

backinghosts = prepare_instances(nbacking, ec2.INSTANCE_TYPE_BACKING, ['role', 'backing'])
cachehosts = prepare_instances(ncaching, ec2.INSTANCE_TYPE_CACHE, ['role', 'cache'])
clienthosts = prepare_instances(ngroups, ec2.INSTANCE_TYPE_CLIENT, ['role', 'client'])

# TODO: make an AMI with this stuff in it
debs = ("build-essential autoconf libtool git libev-dev libjemalloc-dev " +
        "flex bison libboost-dev libboost-thread-dev libboost-system-dev")

prepcmd = "sudo apt-get -qq update; sudo apt-get -y install " + debs + "; " + \
          "echo -e 'Host am.csail.mit.edu\n\tStrictHostKeyChecking no\n' >> ~/.ssh/config;" + \
          "git clone " + user + "@am.csail.mit.edu:/home/am0/eddietwo/pequod.git; " + \
          "cd pequod; git checkout multi2; autoconf < configure.ac; ./bootstrap.sh; ./configure --with-malloc=jemalloc; make"

prepprocs = []
for n in newmachines:
    n.update(True)

    print "Preparing new instance " + n.public_dns_name + "."
    #ec2.scp_to(n.public_dns_name, ".ssh", "scripts/id_rsa_ec2")
    prepprocs.append(ec2.run_ssh_command_bg(n.public_dns_name, prepcmd))
                     
for p in prepprocs:
    p.wait()

prepprocs = []
for h in backinghosts + cachehosts + clienthosts:
    print "Updating instance " + h['dns'] + "."
    prepprocs.append(ec2.run_ssh_command_bg(h['dns'], "cd pequod; git pull; make"))
    
for p in prepprocs:
    p.wait()

for x in exps:
    expdir = None
    
    for e in x['defs']:
        expname = e['name'] if 'name' in e else None
        
        # skip this experiment if it doesn't match an argument
        if args and not [argmatcher for argmatcher in args if fnmatch.fnmatch(x['name'], argmatcher)]:
            continue
        elif not args and e.get("disabled"):
            continue

        (expdir, resdir) = prepare_experiment(x["name"], expname)
        remote_resdir = os.path.join("pequod", resdir)
        
        part = options.part if options.part else e['def_part']
        serverargs = " -H=" + hostpath + " -B=" + str(nbacking) + " -P=" + part

        print "Running experiment" + ((" '" + expname + "'") if expname else "") + \
              " in test '" + x['name'] + "'."
        
        for h in backinghosts + cachehosts + clienthosts:
            ec2.run_ssh_command(h['dns'], "killall pqserver")
            ec2.run_ssh_command(h['dns'], "mkdir -p " + remote_resdir)
            ec2.scp_to(h['dns'], os.path.join("pequod", hostpath), hostpath)

        serverprocs = []
        for s in range(nservers):
            servercmd = e['backendcmd'] if s < nbacking else e['cachecmd']
            host = backinghosts[s] if s < nbacking else cachehosts[s - nbacking]
             
            outfile = os.path.join(resdir, "output_srv_")
            fartfile = os.path.join(resdir, "fart_srv_")
            
            full_cmd = servercmd + serverargs + \
                " -kl=" + str(port) + \
                " > " + outfile + str(s) + ".txt" + \
                " 2> " + fartfile + str(s) + ".txt"

            print full_cmd
            serverprocs.append(ec2.run_ssh_command_bg(host['dns'], "cd pequod; " + full_cmd))

        sleep(3)

        if 'initcmd' in e:
            print "Initializing cache servers."
            initcmd = e['initcmd'] + " -H=" + hostpath + " -B=" + str(nbacking)
            fartfile = os.path.join(resdir, "fart_init.txt")
            
            full_cmd = initcmd + " 2> " + fartfile

            print full_cmd
            ec2.run_ssh_command(clienthosts[0]['dns'], "cd pequod; " + full_cmd)

        if 'populatecmd' in e:
            print "Populating backend."
            procs = []
            popcmd = e['populatecmd'] + " -H=" + hostpath + " -B=" + str(nbacking)
            fartfile = os.path.join(resdir, "fart_pop.txt")
            
            full_cmd = popcmd + " 2> " + fartfile

            print full_cmd
            ec2.run_ssh_command(clienthosts[0]['dns'], "cd pequod; " + full_cmd)

        print "Starting app clients."
        clientcmd = e['clientcmd'] + " -H=" + hostpath + " -B=" + str(nbacking)
        
        clientprocs = []
        for c in range(ngroups):
            outfile = os.path.join(resdir, "output_app_")
            fartfile = os.path.join(resdir, "fart_app_")
            
            full_cmd = clientcmd + \
                " --ngroups=" + str(ngroups) + " --groupid=" + str(c) + \
                " > " + outfile + str(c) + ".json" + \
                " 2> " + fartfile + str(c) + ".txt"

            print full_cmd
            clientprocs.append(ec2.run_ssh_command_bg(clienthosts[c]['dns'], 
                                                      "cd pequod; " + full_cmd))
            
        # wait for clients to finish
        for p in clientprocs:
            p.wait()
        
        # kill servers
        for p in serverprocs:
            p.kill()
        
        # get results
        for h in backinghosts + cachehosts + clienthosts:
            ec2.scp_from(h['dns'], remote_resdir, os.path.join(resdir, os.pardir))
        
        if ngroups > 1:
            aggregate_dir(resdir)
    
        print "Done experiment. Results are stored at", resdir
    
    if expdir and 'plot' in x:
        make_gnuplot(x['name'], expdir, x['plot'])
        
# cleanup
for r in running:
    r.remove_tag('role')

if terminate:
    ec2.terminate_machines(running)
else:
    ec2.shutdown_machines(running)
