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
# we will set a post limit of 200K to stop the experiment
# keep the post:check ratio at 1:49
cmdbase = "./obj/pqserver --client=7007 --twitternew --graph=twitter_graph_1.8M.dat --pactive=70 --postlimit=200000 --popduration=100000 --duration=100000000"
microcmdbase = "./obj/pqserver --client=7007 --rwmicro --nusers=1794167  --pactive=70 --nfollower=41 --postlen=5 --popduration=100000 --prefresh=98 --nops=10000000"
# rwmicro and twitternew use different params, so m[1] and m[2] differ
modes = [["autopush", "", "--push"], ["pull", "--pull", "--no-push"], ["push", "--push", "--client_push"]]
real_twitter = []
for m in modes:
    real_twitter.append({'plotgroup': "%s" % m[0],
                         'plotkey' : "micro",
                         'server' : "./obj/pqserver -kl=7007",
                         'cmd': "%s %s" % (microcmdbase, m[2])});
    real_twitter.append({'plotgroup': "%s" % m[0],
                         'plotkey' : "base",
                         'server' : "./obj/pqserver -kl=7007",
                         'cmd': "%s %s --ppost=%d --pread=%d --plogin=%d --plogout=%d --psubscribe=%d" % (cmdbase, m[1], 1, 49, 0, 0, 0)});
    real_twitter.append({'plotgroup': "%s" % m[0],
                         'plotkey' : "all",
                         'server' : "./obj/pqserver -kl=7007",
                         'cmd': "%s %s --ppost=%d --pread=%d --plogin=%d --plogout=%d --psubscribe=%d" % (cmdbase, m[1], 1, 44, 5, 5, 10)});
exps.append({'name': "real_twitter", 'defs': real_twitter, 'xlabel': ''})

# redis_hackernews. Document here.
cmd = "./obj/pqserver --hn --redis --nops=1000000 --large"
server = "cd scripts/redis-run; bash start.sh"
