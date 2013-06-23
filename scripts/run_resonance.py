#!/usr/bin/env python

import resonanceexperiments
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
from datetime import datetime

nodes = {
	'resonance':[
			'cuda-1-1.local',
			'cuda-1-2.local',
			'cuda-1-3.local',
			'cuda-1-4.local',
			'cuda-1-5.local',
			'cuda-1-6.local',
			'cuda-1-7.local',
			'cuda-1-8.local',
			'cuda-1-9.local',
			'cuda-1-10.local',
			'cuda-1-11.local',
			'cuda-1-12.local',
			'cuda-1-13.local',
			'cuda-1-14.local',
			'cuda-1-15.local'],
#			'cuda-1-16.local'], # up but reserved for client
	'hpc':[
			'compute-3-0.local',
#			'compute-3-1.local', DOWN
			'compute-3-2.local',
#			'compute-3-3.local', DOWN
			'compute-4-1.local',
			'compute-4-2.local',
			'compute-4-3.local',
			'compute-4-4.local',
			'compute-4-5.local',
			'compute-4-6.local',
			'compute-4-7.local']
#			'compute-4-8.local'] # up but reserved for client
#			'compute-5-0.local'] DOWN
}



parser = OptionParser()
parser.add_option("-b", "--backing", action="store", type="int", dest="nbacking", default=1)
parser.add_option("-c", "--caching", action="store", type="int", dest="ncaching", default=5)
parser.add_option("-C", "--cluster", action="store", type="string", dest="cluster", default='resonance')
parser.add_option("-g", "--cgroups", action="store", type="int", dest="cgroups", default=1)
parser.add_option("-p", "--startport", action="store", type="int", dest="startport", default=9000)
parser.add_option("-a", "--affinity", action="store_true", dest="affinity", default=False)
parser.add_option("-A", "--startcpu", action="store", type="int", dest="startcpu", default=0)
parser.add_option("-P", "--perfserver", action="store", type="int", dest="perfserver", default=-1)
parser.add_option("-f", "--part", action="store", type="string", dest="part", default=None)
(options, args) = parser.parse_args()

nbacking = options.nbacking
ncaching = options.ncaching
poolname = options.cluster
groupcount = options.cgroups
startport = options.startport
affinity = options.affinity
startcpu = options.startcpu
perfserver = options.perfserver
count = 0

nprocesses = nbacking + ncaching
hosts = nodes['resonance'] if poolname == 'resonance' else nodes['hpc'] 
availablehostcount = len(hosts)
cachehostcount = availablehostcount - nbacking
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
				if cachehostcount < 1:
					print "No cache hosts available, adjust -c -b"
					exit()
				for h in range(nprocesses):
						if h < nbacking:
							hfile.write(hosts[h] + "\t" + str(startport) + "\n");
						else:
							portoffset = (h - nbacking)/cachehostcount
							hostoffset = ((h - nbacking)%cachehostcount) + nbacking
							hfile.write(hosts[hostoffset] + "\t" + str(startport + portoffset) + "\n");

				hfile.close()

		resdir = os.path.join(uniquedir, xname, ename)
		os.makedirs(resdir)
		return resdir

exps = resonanceexperiments.exps
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

						if s < nbacking:
							portoffset = 0
							hostoffset = s
						else:
							portoffset = (s- nbacking)/cachehostcount
							hostoffset = ((s - nbacking)%cachehostcount) + nbacking

						full_cmd = pin + perf + servercmd + \
								" -kl=" + str(startport + portoffset) + \
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
						ssh_cmd = 'ssh -x %s "./start_pequod_srv%d.sh"' % (hosts[hostoffset], s)
						print ssh_cmd
						call([ssh_cmd], shell=True)

				sleep(3)

				print "Populating store..."
				clientcmd = e['populatecmd'] + " -H=" + hostpath + " -B=" + str(nbacking) # + " --ngroups=" + str(groupcount) + " --groupid=" str(1)
				 
				fartfile = os.path.join(resdir, "fart_pop.txt")
				pop_cmd = clientcmd + " > " + fartfile + " 2>&1"

				print pop_cmd
				start = datetime.now()
				pop_pid = Popen(pop_cmd, shell=True);

				pop_pid.wait()

				end = datetime.now()
				
				print "Completed (%s)" % (end-start)

				print "Starting app clients."
				procs = []
				clientcmd = e['clientcmd'] + " -H=" + hostpath + " -B=" + str(nbacking) + " --ngroups=" + str(groupcount) + " --groupid=" + str(0)
				
				for c in range(groupcount):
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
		
				end = datetime.now()
				print "Completed (%s)" % (end-start)

				print "Cleaning up..."
				call(["rm -f " + homedir + "/start_pequod_srv*"], shell=True)
				used_srv_cnt = nprocesses if nprocesses < len(hosts) else len(hosts)
				for s in range(used_srv_cnt):
						print "Killing %s" % (hosts[s])
						ssh_cmd = 'ssh -x %s "killall pqserver"' % (hosts[s])
						call([ssh_cmd], shell=True)

				call(["scripts/aggregate_json.py " + resdir], shell=True)

				print "Experiment complete. Results are stored at", resdir
