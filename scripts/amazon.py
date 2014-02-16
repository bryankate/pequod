#!/usr/bin/env python

import sys
import os
import math
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
parser.add_option("-L", "--link", action="store", type="string", dest="symlink", default=None)
parser.add_option("-b", "--backing", action="store", type="int", dest="nbacking", default=1)
parser.add_option("-B", "--clusterbacking", action="store", type="int", dest="cbacking", default=1)
parser.add_option("-c", "--caching", action="store", type="int", dest="ncaching", default=5)
parser.add_option("-C", "--clustercaching", action="store", type="int", dest="ccaching", default=1)
parser.add_option("-f", "--part", action="store", type="string", dest="part", default=None)
parser.add_option("-g", "--clientgroups", action="store", type="int", dest="ngroups", default=1)
parser.add_option("-G", "--clustergroups", action="store", type="int", dest="cgroups", default=1)
parser.add_option("-t", "--terminate", action="store_true", dest="terminate", default=False)
parser.add_option("-P", "--preponly", action="store_true", dest="preponly", default=False)
parser.add_option("-N", "--noprep", action="store_true", dest="noprep", default=False)
parser.add_option("-K", "--kill", action="store_true", dest="kill", default=False)
parser.add_option("-A", "--keepalive", action="store_true", dest="keepalive", default=False)
parser.add_option("-R", "--keeproles", action="store_true", dest="keeproles", default=False)
parser.add_option("-i", "--invoke", action="store", type="string", dest="invoke", default=None)
parser.add_option("-F", "--filter", action="store", type="string", dest="filter", default=None)
parser.add_option("-D", "--ondemand", action="store_true", dest="ondemand", default=False)
parser.add_option("-u", "--user", action="store", type="string", dest="user", default=getpass.getuser())
(options, args) = parser.parse_args()

expfile = options.expfile
symlink = options.symlink
nbacking = options.nbacking
cbacking = options.cbacking
ncaching = options.ncaching
ccaching = options.ccaching
ngroups = options.ngroups
cgroups = options.cgroups
terminate = options.terminate
preponly = options.preponly
noprep = options.noprep
justkill = options.kill
invoke = options.invoke
filter = options.filter
ondemand = options.ondemand
keepalive = options.keepalive
keeproles = options.keeproles
user = options.user
startport = 9000

topdir = None
uniquedir = None
hostpath = None

# load experiment definitions as global 'exps'
exph = open(expfile, "r")
exec(exph, globals())
exph.close()

def kill():
    print "Going nuclear on EC2"
    ec2.terminate_machines(ec2.get_all_instances())
    ec2.cancel_spot_requests(ec2.get_all_spot_requests())
    exit(0)

ec2.connect()

if justkill:
    kill()

if invoke:
    running = ec2.get_running_instances(tag=['role', filter] if filter else None)
    for r in running:
        ec2.run_ssh_command(r.public_dns_name, invoke)
    exit(0)

running = []
newmachines = []

def startup_instances(num, cluster, type, roletag):
    global running, newmachines
    
    if num == 0:
        return []
    
    machines = int(math.ceil(num / float(cluster)))
    running_ = ec2.get_running_instances(type, ['role', roletag])
    pending_ = []
    
    if len(running_) < machines:
        running_ += ec2.get_running_instances(type, ['role', 'none'])
        
    if len(running_) < machines:
        print "Adding machines for role: " + roletag
        nstarted = len(running_)
        
        stopped_ = ec2.get_stopped_instances(type)
        if stopped_:
            ntostart = min(len(stopped_), machines - nstarted)
            ec2.startup_machines(stopped_[0:ntostart])
            pending_ += stopped_[0:ntostart]
            nstarted += ntostart
        
        if nstarted < machines:
            if ondemand:
                checkedout = ec2.checkout_machines(machines - nstarted, type)
            else:
                requests = ec2.request_machines(machines - nstarted, type)
                rids = [r.id for r in requests]

                while(True):
                    print "Waiting for spot instance fulfillment..."
                    open = False

                    requests = ec2.update_spot_requests(rids)
                    if len(requests) != len(rids):
                        print "Problem updating spot request state!"
                        kill()
                    
                    for r in requests:
                        if r.state == 'active':
                            continue
                        elif r.state == 'open':
                            if r.status == 'price-too-low':
                                print "You bid too low!"
                                kill()
                            open = True
                        else:
                            print "Bad spot request state: (" + r.id + ", " + r.state + ")"
                            kill()
                    if not open:
                        break
                    sleep(10)
                    
                checkedout = ec2.get_instances([r.instance_id for r in requests])
            
            pending_ += checkedout
            newmachines += checkedout
    else:
        print "Using existing machines for role: " + roletag
    
    while (pending_):
        print "Waiting for machines to start..."
        again = []
        
        for p in pending_:
            if ec2.is_running(p):
                p.add_tag('role', roletag)
            else:
                again.append(p)
        
        pending_ = again
        sleep(5)
        
    hosts_ = []
    running_ = ec2.get_running_instances(type, ['role', roletag]) + \
               ec2.get_running_instances(type, ['role', 'none'])
    running_ = running_[0:machines]
    running += running_
    
    for r in running_:
        r.update(True)
        r.add_tag('role', roletag)
        
    return running_

def add_server_hosts(hfile, num, cluster, hosts):
    if num == 0:
        return
    
    global serverconns
    assert len(hosts) >= num / cluster
    
    h = 0
    c = 0
    for s in range(num):
        hfile.write(hosts[h].private_ip_address + "\t" + str(startport + s) + "\n")
        serverconns.append([hosts[h].public_dns_name, startport + s])
        c += 1
        if c % cluster == 0:
            h += 1 

def prepare_experiment(xname, ename):
    global topdir, uniquedir, hostpath, serverconns
    serverconns = []
    
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

        hostpath = os.path.join(uniquedir, "hosts.txt")
        hfile = open(hostpath, "w")
        add_server_hosts(hfile, nbacking, cbacking, backinghosts)
        add_server_hosts(hfile, ncaching, ccaching, cachehosts)
        hfile.close()

    resdir = os.path.join(uniquedir, xname, ename if ename else "")
    os.makedirs(resdir)
    return (os.path.join(uniquedir, xname), resdir)

def prepare_instances(instances, test, cmd, log):
    procs = []
    
    for p in instances:
        p.update(True)
        if ec2.run_ssh_command(p.public_dns_name, test) != 0:
            print log + " on instance " + p.id + " (" + p.public_dns_name + ")."
            procs.append(ec2.run_ssh_command_bg(p.public_dns_name, cmd))
            
    for p in procs:
        p.wait()



cachehosts = startup_instances(ncaching, ccaching, ec2.INSTANCE_TYPE_CACHE, 'cache')
backinghosts = startup_instances(nbacking, cbacking, ec2.INSTANCE_TYPE_BACKING, 'backing')
clienthosts = startup_instances(ngroups, cgroups, ec2.INSTANCE_TYPE_CLIENT, 'client')
serverconns = []

remote_tmpdir = "/mnt/pequod"

if not noprep:
    print "Testing SSH connections."
    for h in running:
        while ec2.run_ssh_command(h.public_dns_name, "echo 2>&1 \"%s is alive\"" % (h.public_dns_name)) != 0:
            print "Waiting for SSH access to " + h.public_dns_name
            sleep(5)

    debs = ("htop iftop numactl build-essential gdb valgrind autoconf libtool " + 
            "git libev-dev libjemalloc-dev flex bison " +
            "libboost-dev libboost-thread-dev libboost-system-dev")

    installcmd = "sudo mkdir -p " + remote_tmpdir + \
                 "; sudo chown ubuntu:ubuntu " + remote_tmpdir + \
                 "; sudo chmod 777 " + remote_tmpdir + \
                 "; mkdir -p " + os.path.join(remote_tmpdir, "cores") + \
                 "; sudo bash -c 'echo " + os.path.join(remote_tmpdir, "cores", "core.%p") + " > /proc/sys/kernel/core_pattern'" + \
                 "; sudo apt-get -qq update; sudo apt-get -y install " + debs

    buildcmd = "echo -e 'Host am.csail.mit.edu\n\tStrictHostKeyChecking no\n' >> ~/.ssh/config; " + \
               "git clone " + user + "@am.csail.mit.edu:/home/am0/eddietwo/pequod.git; " + \
               "cd pequod; git checkout bk-master; autoconf < configure.ac; " + \
               "./bootstrap.sh; ./configure --with-malloc=jemalloc; make; exit"

    graph = 'twitter_graph_40M.dat'
    graphcmd = "cd " + remote_tmpdir + "; wget -nv http://www.eecs.harvard.edu/~bkate/tmp/pequod/" + graph + ".tar.gz; " + \
               "tar -zxf " + graph + ".tar.gz; chmod 666 twitter*; exit"


    print "Checking instance preparedness."
    prepare_instances(running, "gcc --version > /dev/null", installcmd, "Installing software")
    prepare_instances(running, "[ -x pequod/obj/pqserver ]", buildcmd, "Building pequod")
    #prepare_instances(clienthosts, "[ -e " + os.path.join(remote_tmpdir, graph) + " ]", graphcmd, "Fetching Twitter graph")

    for h in running:
        print "Updating instance " + h.id + " (" + h.public_dns_name + ")."
        ec2.run_ssh_command(h.public_dns_name, "cd pequod; git pull -q; make -s; exit")

    print "Checking that pequod is built on all servers."
    pqexists = True
    for h in running:
        if ec2.run_ssh_command(h.public_dns_name, "[ -x pequod/obj/pqserver ]") != 0:
            print "Instance " + h.id + " (" + h.public_dns_name + ") does not have pqserver!"
            pqexists = False

    if not pqexists:        
        exit(-1)

if (preponly):
    exit(0)

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
        remote_resdir = os.path.join(remote_tmpdir, resdir)
        remote_hostpath = os.path.join(remote_tmpdir, hostpath)
        logfd = open(os.path.join(resdir, "cmd_log.txt"), "w")
            
        print "Running experiment" + ((" '" + expname + "'") if expname else "") + \
              " in test '" + x['name'] + "'."
        
        for h in backinghosts + cachehosts + clienthosts:
            ec2.run_ssh_command(h.public_dns_name, 
                                "killall pqserver" + \
                                "; mkdir -p " + remote_resdir + \
                                "; rm -f last; ln -s " + remote_resdir + " last")
            ec2.scp_to(h.public_dns_name, remote_hostpath, hostpath)

        part = options.part if options.part else e['def_part']
        serverargs = " -H=" + remote_hostpath + " -B=" + str(nbacking) + " -P=" + part
        outfile = os.path.join(remote_resdir, "output_srv_")
        fartfile = os.path.join(remote_resdir, "fart_srv_")

        serverprocs = []
        for s in range(nbacking + ncaching):
            servercmd = e['backendcmd'] if s < nbacking else e['cachecmd']
            conn = serverconns[s]
            
            full_cmd = servercmd + serverargs + \
                " -kl=" + str(conn[1]) + \
                " > " + outfile + str(s) + ".txt" + \
                " 2> " + fartfile + str(s) + ".txt"

            print full_cmd
            logfd.write(conn[0] + ": " + full_cmd + "\n")
            logfd.flush()
            serverprocs.append(ec2.run_ssh_command_bg(conn[0], "cd pequod; ulimit -c unlimited; " + full_cmd))

        sleep(30)

        if 'initcmd' in e:
            print "Initializing cache servers."
            initcmd = e['initcmd'] + " -H=" + remote_hostpath + " -B=" + str(nbacking)
            fartfile = os.path.join(remote_resdir, "fart_init.txt")
            
            full_cmd = initcmd + " 2> " + fartfile

            print full_cmd
            logfd.write(clienthosts[0].public_dns_name + ": " + full_cmd + "\n")
            logfd.flush()
            ec2.run_ssh_command(clienthosts[0].public_dns_name, "cd pequod; ulimit -c unlimited; " + full_cmd)

        sleep(5)

        if 'populatecmd' in e:
            print "Populating backend."
            popcmd = e['populatecmd'] + " -H=" + remote_hostpath + " -B=" + str(nbacking)
            fartfile = os.path.join(remote_resdir, "fart_pop_")

            clientprocs = []
            for c in range(ngroups):         
                full_cmd = popcmd + \
                           " --ngroups=" + str(ngroups) + " --groupid=" + str(c) + \
                           " 2> " + fartfile + str(c) + ".txt"
    
                print full_cmd
                chost = clienthosts[c % len(clienthosts)].public_dns_name
                logfd.write(chost + ": " + full_cmd + "\n")
                logfd.flush()
                clientprocs.append(ec2.run_ssh_command_bg(chost, "cd pequod; ulimit -c unlimited; " + full_cmd))
            
            for p in clientprocs:
                p.wait()

        sleep(5)

        print "Starting app clients."
        clientcmd = e['clientcmd'] + " -H=" + remote_hostpath + " -B=" + str(nbacking)
        outfile = os.path.join(remote_resdir, "output_app_")
        fartfile = os.path.join(remote_resdir, "fart_app_")
            
        clientprocs = []
        for c in range(ngroups):
            full_cmd = clientcmd + \
                " --ngroups=" + str(ngroups) + " --groupid=" + str(c) + \
                " --master-host=" + clienthosts[0].private_ip_address + \
                " > " + outfile + str(c) + ".json" + \
                " 2> " + fartfile + str(c) + ".txt"

            print full_cmd
            chost = clienthosts[c % len(clienthosts)].public_dns_name
            logfd.write(chost + ": " + full_cmd + "\n")
            logfd.flush()
            clientprocs.append(ec2.run_ssh_command_bg(chost, "cd pequod; ulimit -c unlimited; " + full_cmd))
            
        # wait for clients to finish
        for p in clientprocs:
            p.wait()
        
        # kill servers
        for p in serverprocs:
            p.kill()
        
        # get results
        print "Gathering results."
        for h in running:
            ec2.scp_from(h.public_dns_name, remote_resdir, os.path.join(resdir, os.pardir))
        
        if ngroups > 1:
            aggregate_dir(resdir, x['name'])
    
        print "Done experiment. Results are stored at", resdir
    
    if expdir and 'plot' in x:
        make_gnuplot(x['name'], expdir, x['plot'])
        
# cleanup
if not keeproles:
    for r in running:
        r.add_tag('role', 'none')

if not keepalive:
    if terminate:
        ec2.terminate_machines(running)
    else:
        ec2.shutdown_machines(running)
