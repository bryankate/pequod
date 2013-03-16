#!/usr/bin/python

import subprocess, sys, time, os, shutil, getpass, re
from sets import Set
from optparse import OptionParser
from lib import config
from lib.remoterunner import RemoteRunner
from lib import remoterunnermgr
from lib.process import Process
from pequod import PequodLoadRunner
import experiments
from pequod_analyzer import ResultAnalyzer

allExpr = experiments.exps

resultDir = "results/incomplete"
if os.path.exists(resultDir):
    shutil.rmtree(resultDir)
os.makedirs(resultDir)

def remoteRun(cmd, hosts, builtin = False):
    runners = []
    for h in hosts:
        n = RemoteRunner(h, 0, resultDir = resultDir)
        runners.append(n)
        if not builtin:
            n.send_command(cmd)
        else:
            getattr(n, cmd)()
    if remoterunnermgr.waitAll(runners, sys.stdout):
        raise Exception("Error! See detailed error in %s" % resultDir)

parser = OptionParser()
parser.add_option("-l", "--runLoads", dest = "loads", default = None,
                  type = "string", help = "A comma separated loads to run")
parser.add_option("-e", "--runExpr", dest = "expr", default = "rwmicro",
                 type = "string", help = "Run an experiment")
parser.add_option("-u", "--user", dest = "user", default = getpass.getuser(),
                  type = "string",
                  help = "ssh user")
parser.add_option("--cluster", dest = "cluster",
                  default = "pwd", type = "string",
                  help = "cluster to run the experiment on")
parser.add_option("--perf", action="store_true", dest="perf", default=False)

# The following options are processed by lib/pequod.py
parser.add_option("-r", "--repeat", action="store", type="int", dest="repeat", default=1)
parser.add_option("-a", "--affinity", action="store_true", dest="affinity", default=False)

(options, args) = parser.parse_args()
config.USER = options.user
if not config.CLUSTERS.has_key(options.cluster):
    raise Exception('bad cluster name ' + options.cluster)
cluster = config.CLUSTERS[options.cluster]

def getLoadName(load):
    return load['plotgroup'] + '_' + str(load['plotkey'])

try:
    finalDir = os.path.join("results", time.strftime("%Y_%m_%d-%H_%M_%S"))
    if options.expr:
        expr = [x for x in allExpr if x['name'] == options.expr]
        if not expr:
            allExprNames = [x['name'] for x in allExpr]
            raise Exception("Valid experiments are", ", ".join(allExprNames))
        expr = expr[0]
        if options.cluster != 'pwd':
            remoteRun(options.serverConfig if options.serverConfig
                                           else "git_pull_and_make", 
                      [c['addr'] for c in cluster['clients']])
            remoteRun(options.clientConfig if options.clientConfig
                                           else "git_pull_and_make",
                      [s['addr'] for s in cluster['servers']])
        analyzer = ResultAnalyzer(expr['xlabel'])
        exprDir = os.path.join(resultDir, expr['name'])
        loads = [] if not options.loads else options.loads.split(',')
        validLoads = [getLoadName(load) for load in expr['defs']]
        for l in loads:
            if not l in validLoads:
                raise Exception("Unknown load: " + l, 'Candidates: ' + ', '.join(validLoads))
        for load in expr['defs']:
            lname = getLoadName(load)
            if loads and not lname in loads: continue
            loadResultDir = os.path.join(exprDir, lname)
            os.makedirs(loadResultDir)
            PequodLoadRunner.run(expr['name'], load, options, cluster, loadResultDir)
            analyzer.add(load['plotgroup'], loadResultDir, load['plotkey'])
        basename = expr['name'] + '.data'
        analyzer.getGNUData(os.path.join(resultDir, basename),
                            resultDir, os.path.join(finalDir, basename))
        analyzer.getCSVHorizon("results/notebook.csv",
                               resultDir, os.path.join(finalDir, basename))
    elif options.machines:
        for s in cluster['servers']:
            print "Server: %s" % s['addr']
        for c in cluster['clients']:
            print "Client: %s" % c['addr']
    elif options.clientConfig or options.serverConfig:
        if options.cluster == 'pwd':
            raise Exception('If you want to do something at current directory, do it by yourself!')
        if options.clientConfig:
            remoteRun(options.clientConfig,
                      [c['addr'] for c in cluster['clients']])
        if options.serverConfig:
            remoteRun(options.serverConfig,
                      [s['addr'] for s in cluster['servers']])
    else:
        parser.print_help()
        sys.exit(-1)

    shutil.move(resultDir, finalDir)
    # Use lexists to test the existence of symbolic link.
    if os.path.lexists("last"):
        os.unlink("last")
    os.symlink(finalDir, "last")
    print "Test OK. See detailed results in", finalDir, " or last for convienience"
except:
    print "Error! See detailed error in", resultDir
    raise
