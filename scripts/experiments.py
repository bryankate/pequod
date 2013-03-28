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

# hash_user
cmdbase = "./obj/pqserver --nops=2000000 --rwmicro --popduration=100000 "
nusers = [2000, 4000, 8000, 16000, 32000, 64000]
ehash_user = []
for nuser in nusers:
    ehash_user.append(
        {'plotgroup': "Hash",
         'plotkey' : nuser,
         'cmd': "%s --prefresh=80 --nusers=%d --client_push -b" % (cmdbase, nuser)})
    ehash_user.append(
        {'plotgroup': "Pequod",
         'plotkey' : nuser,
         'cmd': "%s --prefresh=80 --nusers=%d" % (cmdbase, nuser)})

exps.append({'name': 'ehash_user', 'defs': ehash_user, 'xlabel' : 'Number of users'})

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
cmdbase = "./obj/pqserver --nusers=2000 --nops=2000000 --rwmicro "
prefresh = [0, 5, 10, 20, 40, 60, 80, 90, 95, 100]
for nfollower in [1, 16, 32]:
    client_push = []
    cmd = cmdbase + ("--nfollower=%d" % nfollower)
    for pr in prefresh:
        client_push.append(
            {'plotgroup': "local_push",
             'plotkey' : pr,
             'cmd': "%s --prefresh=%d" % (cmd, pr)})
        client_push.append(
            {'plotgroup': "local_client_push",
             'plotkey' : pr,
             'cmd': "%s --prefresh=%d --client_push" % (cmd, pr)})

        client_push.append(
            {'plotgroup': "remote_push",
             'plotkey' : pr,
             'server' : "./obj/pqserver -kl9901",
             'cmd': "%s --prefresh=%d --client=9901" % (cmd, pr)})
        client_push.append(
            {'plotgroup': "remote_client_push",
             'plotkey' : pr,
             'server' : "./obj/pqserver -kl9901",
             'cmd': "%s --prefresh=%d --client=9901 --client_push" % (cmd, pr)})
    ename = "remote_client_push_%d" % nfollower
    exps.append({'name': ename, 'defs': client_push, 'xlabel' : 'refresh ratio (%)'})

# real_twitter
# we will set a post limit of 200K to stop the experiment
# keep the post:check ratio at 1:49
cmdbase = "./obj/pqserver --client=7007 --twitternew --graph=twitter_graph_1.8M.dat --pactive=70 --postlimit=200000 --popduration=100000 --duration=100000000"
#modes = [["autopush", ""], ["pull", "--pull"], ["push", "--push"]]
modes = [["autopush", ""]]
real_twitter = []
for m in modes:
    real_twitter.append({'plotgroup': "%s" % m[0],
                         'plotkey' : "base",
                         'server' : "./obj/pqserver -kl=7007",
                         'cmd': "%s %s --ppost=%d --pread=%d --plogin=%d --plogout=%d --psubscribe=%d" % (cmdbase, m[1], 1, 49, 0, 0, 0)});
    real_twitter.append({'plotgroup': "%s" % m[0],
                         'plotkey' : "all",
                         'server' : "./obj/pqserver -kl=7007",
                         'cmd': "%s %s --ppost=%d --pread=%d --plogin=%d --plogout=%d --psubscribe=%d" % (cmdbase, m[1], 1, 44, 5, 5, 10)});
    real_twitter.append({'plotgroup': "%s" % m[0],
                         'plotkey' : "celebrity",
                         'server' : "./obj/pqserver -kl=7007",
                         'cmd': "%s %s --celebrity=5000 --ppost=%d --pread=%d --plogin=%d --plogout=%d --psubscribe=%d" % (cmdbase, m[1], 1, 44, 5, 5, 10)});
exps.append({'name': "real_twitter", 'defs': real_twitter, 'xlabel': ''})

# redis_hackernews. Document here.
cmdbase = "./obj/pqserver --hn --redis --nops=1000000 --large"
server = "cd scripts/redis-run; bash start.sh"
redis_hn = []
redis_hn.append(
    {'plotgroup': "remote_push",
     'plotkey': "Pequod_hn",
     'server': "./obj/pqserver -kl9901",
     'cmd' : '%s --client=9901' % cmdbase})
redis_hn.append(
    {'plotgroup': "redis",
     'plotkey': "hn",
     'server': "cd scripts/redis-run; bash start.sh",
     'cmd' : '%s' % cmdbase})
exps.append({'name' : 'redis_hn', 'defs': redis_hn, 'xlabel' : 'System'})


# hackernews changing vote rate.
cmdbase = "./obj/pqserver --hn --nops=4000000 --large"
vote_hn = []
vote_rate = [0, 5, 10, 20, 50]
for vr in vote_rate:
    vote_hn.append(
        {'plotgroup': "karma",
         'plotkey': vr,
         'cmd' : '%s --vote_rate=%d' % (cmdbase, vr)})
    vote_hn.append(
        {'plotgroup': "ma",
         'plotkey': vr,
         'cmd' : '%s --vote_rate=%d --super_materialize' % (cmdbase, vr)})
exps.append({'name' : 'vote_hn', 'defs': vote_hn, 'xlabel' : 'Vote Rate'})


# redis_micro
cmdbase = "./obj/pqserver --nusers=2000 --nops=2000000 --rwmicro --nfollower=16 --prefresh=50"
redis_micro = []
redis_micro.append(
    {'plotgroup': "remote_push",
     'plotkey' : "Pequod_micro",
     'server' : "./obj/pqserver -kl9901",
     'cmd': '%s --client=9901' % cmdbase})
redis_micro.append(
    {'plotgroup': "redis",
     'plotkey' : "micro",
     'server' : "cd scripts/redis-run; bash start.sh",
     'cmd' : '%s --client_push --redis ' % cmdbase })
exps.append({'name': 'redis_micro_16', 'defs': redis_micro, 'xlabel' : 'System'})

# network analytics
cmdbase = "./obj/pqserver --analytics --popduration=10000 --duration=1000000 --proactive"
analytics = []
analytics.append(
    {'plotgroup': "analytics",
     'plotkey' : "base",
     'server' : "./obj/pqserver -kl=7008",
     'cmd': '%s --client=7008' % cmdbase})
exps.append({'name': 'analytics', 'defs': analytics, 'xlabel' : ''})

# breakdown
cmdbase = "./obj/pqserver --nusers=2000 --nops=2000000 --rwmicro --nfollower=16 "
breakdown = []
prefresh = [0, 5, 10, 20, 40, 60, 80, 90, 95, 100]
for pr in prefresh:
    breakdown.append(
        {'plotgroup' : 'Base',
         'plotkey' : pr,
         'build' : './configure --disable-hint --disable-value-sharing; make -j8',
         'server' : './obj/pqserver -kl=7277',
         'cmd': '%s --client=7277 --prefresh=%d' % (cmdbase, pr) })
    breakdown.append(
        {'plotgroup' : 'Base_no_RPC',
         'plotkey' : pr,
         'build' : './configure --disable-hint --disable-value-sharing; make -j8',
         'cmd': '%s --prefresh=%d' % (cmdbase, pr) })
    breakdown.append(
        {'plotgroup' : 'Hint_no_RPC',
         'plotkey' : pr,
         'build' : './configure --disable-value-sharing; make -j8',
         'cmd': '%s --prefresh=%d' % (cmdbase, pr) })
    breakdown.append(
        {'plotgroup' : 'Hint_Value_Sharing_no_RPC',
         'plotkey' : pr,
         'build' : './configure ; make -j8',
         'cmd': '%s --prefresh=%d' % (cmdbase, pr) })

exps.append({'name': 'breakdown', 'defs': breakdown, 'xlabel' : 'Configuration'})

# bar graph for remote_client_push v.s Pequod.
# Each refresh returns 20 tweets (i.e. with 45% refresh ratio)
cmdbase = "./obj/pqserver --nusers=2000 --nops=2000000 --rwmicro --nfollower=16 --prefresh=45"
bar = []
bar.append(
    {'plotgroup' : 'Pequod',
     'plotkey' : 45,
     'server' : './obj/pqserver -kl=7277',
     'cmd': '%s --client=7277' % cmdbase })
bar.append(
    {'plotgroup' : 'Client_push',
     'plotkey' : 45,
     'server' : './obj/pqserver -kl=7277',
     'cmd': '%s --client=7277 --client_push' % cmdbase })
exps.append({'name' : 'remote_client_push_16_bar', 'defs' : bar, 'xlabel' : "System"})

# breakdown_bar
cmdbase = "./obj/pqserver --nusers=2000 --nops=2000000 --rwmicro --nfollower=16 --prefresh=45"
breakdown_bar = []
breakdown_bar.append(
    {'plotgroup' : 'Base',
     'plotkey' : 45,
     'build' : './configure --disable-hint --disable-value-sharing; make -j8',
     'server' : './obj/pqserver -kl=7277',
     'cmd': '%s --client=7277' % cmdbase })
breakdown_bar.append(
    {'plotgroup' : 'Hint',
     'plotkey' : 45,
     'build' : './configure --disable-value-sharing; make -j8',
     'server' : './obj/pqserver -kl=7277',
     'cmd': '%s --client=7277' % cmdbase })
breakdown_bar.append(
    {'plotgroup' : 'Hint_Value_Sharing',
     'plotkey' : 45,
     'build' : './configure ; make -j8',
     'server' : './obj/pqserver -kl=7277',
     'cmd': '%s --client=7277' % cmdbase })
breakdown_bar.append(
    {'plotgroup' : 'Hint_Value_Sharing_no_RPC',
     'plotkey' : 45,
     'build' : './configure ; make -j8',
     'cmd': '%s' % cmdbase })

exps.append({'name': 'breakdown_bar', 'defs': breakdown_bar, 'xlabel' : 'Configuration'})

