# open loop test definitions
import copy, math

exps = []
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

