# open loop test definitions
import copy, math

exps = []
# server pull and server push
cmdbase = "./obj/pqserver --nusers=2000 --nops=2000000 --rwmicro "
prefresh = [0, 5, 10, 20, 40, 60, 80, 90, 95, 100]
for i in range(0, 6):
    nfollower = math.pow(2, i)
    rwmicro = []
    cmd = cmdbase + ("--nfollower=%d" % nfollower)
    for pr in prefresh:
        rwmicro.append(
            {'plotgroup': "push",
             'plotkey' : pr,
             'cmd': "%s --prefresh=%d --push" % (cmd, pr)})
        rwmicro.append(
            {'plotgroup': "pull",
             'plotkey' : pr,
             'cmd': "%s --prefresh=%d --no-push" % (cmd, pr)})
    ename = "rwmicro_%d" % nfollower
    exps.append({'name': ename, 'defs': rwmicro, 'xlabel' : 'refresh ratio (%)'})

# policy
cmdbase = './obj/pqserver --nusers=2000 --nops=2000000 --rwmicro --nfollower=16 --prefresh=50'
pactive = [100, 95, 90, 80, 60, 40, 20, 10, 5, 1]
epolicy = []
for pa in pactive:
    epolicy.append(
            {'plotgroup': 'push-dynamic',
             'plotkey': pa,
             'cmd': '%s --pactive=%d --pprerefresh=%d' % (cmdbase, pa, pa)})
    epolicy.append(
            {'plotgroup': 'push-all',
             'plotkey': pa,
             'cmd': '%s --pactive=%d --pprerefresh=100' % (cmdbase, pa)})
    epolicy.append(
            {'plotgroup': 'pull',
             'plotkey': pa,
             'cmd': '%s --pactive=%d --no-push' % (cmdbase, pa)})
exps.append({'name': 'policy', 'defs' : epolicy, 'xlabel' : 'inactive users(%)'})

# hash
cmdbase = "./obj/pqserver --nusers=2000 --nops=2000000 --rwmicro "
prefresh = [0, 5, 10, 20, 40, 60, 80, 90, 95, 100]
for nfollower in [1, 16, 32]:
    ehash = []
    cmd = cmdbase + ("--nfollower=%d" % nfollower)
    for pr in prefresh:
        ehash.append(
            {'plotgroup': "Hash",
             'plotkey' : pr,
             'cmd': "%s --prefresh=%d --client_push -b" % (cmd, pr)})
    ename = "ehash_%d" % nfollower
    exps.append({'name': ename, 'defs': ehash, 'xlabel' : 'refresh ratio (%)'})

# client_push
cmdbase = "./obj/pqserver --nusers=2000 --nops=2000000 --rwmicro "
prefresh = [0, 5, 10, 20, 40, 60, 80, 90, 95, 100]
for nfollower in [1, 16, 32]:
    client_push = []
    cmd = cmdbase + ("--nfollower=%d" % nfollower)
    for pr in prefresh:
        client_push.append(
            {'plotgroup': "client_push",
             'plotkey' : pr,
             'cmd': "%s --prefresh=%d --client_push" % (cmd, pr)})
    ename = "client_push_%d" % nfollower
    exps.append({'name': ename, 'defs': client_push, 'xlabel' : 'refresh ratio (%)'})

# remote_client_push
cmdbase = "./obj/pqserver --nusers=2000 --nops=2000000 --rwmicro --client=9901 "
prefresh = [0, 5, 10, 20, 40, 60, 80, 90, 95, 100]
for nfollower in [1, 16, 32]:
    client_push = []
    cmd = cmdbase + ("--nfollower=%d" % nfollower)
    for pr in prefresh:
        client_push.append(
            {'plotgroup': "remote_push",
             'plotkey' : pr,
             'server' : "./obj/pqserver -kl9901",
             'cmd': "%s --prefresh=%d" % (cmd, pr)})
        client_push.append(
            {'plotgroup': "remote_client_push",
             'plotkey' : pr,
             'server' : "./obj/pqserver -kl9901",
             'cmd': "%s --prefresh=%d --client_push" % (cmd, pr)})
    ename = "remote_client_push_%d" % nfollower
    exps.append({'name': ename, 'defs': client_push, 'xlabel' : 'refresh ratio (%)'})

# real_twitter
# we will set a post limit of 150K to stop the experiment
# keep the post:check ratio at 1:65
cmdbase = "./obj/pqserver --twitternew --graph=twitter_graph_1.8M.dat --pactive=70 --postlimit=150000--duration=100000000"
exps.append({'name': "real_twitter", 
             'defs': [{'plotgroup': "real_twitter",
                       'plotkey' : "base",
                       'cmd': "%s --ppost=%d --pread=%d --plogin=%d --plogout=%d --psubscribe=%d" % (cmdbase, 1, 65, 0, 0, 0)},
                      {'plotgroup': "real_twitter",
                       'plotkey' : "login",
                       'cmd': "%s --ppost=%d --pread=%d --plogin=%d --plogout=%d --psubscribe=%d" % (cmdbase, 1, 60, 5, 5, 0)},
                      {'plotgroup': "real_twitter",
                       'plotkey' : "subscribe",
                       'cmd': "%s --ppost=%d --pread=%d --plogin=%d --plogout=%d --psubscribe=%d" % (cmdbase, 1, 65, 0, 0, 10)},
                      {'plotgroup': "real_twitter",
                       'plotkey' : "all",
                       'cmd': "%s --ppost=%d --pread=%d --plogin=%d --plogout=%d --psubscribe=%d" % (cmdbase, 1, 60, 5, 5, 10)}],
             'xlabel': ''})
    