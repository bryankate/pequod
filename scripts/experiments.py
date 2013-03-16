# open loop test definitions
import copy

rwmicro = []
cmdbase = "./obj/pqserver --nusers=10000 --nops=100000 --rwmicro "
prefresh = [0, 20, 40, 60, 80, 100]
for pr in prefresh:
    rwmicro.append(
        {'plotgroup': "push",
         'plotkey' : pr,
         'cmd': "%s --prefresh=%d --push" % (cmdbase, pr)})
    rwmicro.append(
        {'plotgroup': "pull",
         'plotkey' : pr,
         'cmd': "%s --prefresh=%d --no-push" % (cmdbase, pr)})

exps = [{'name': "rwmicro", 'defs': rwmicro, 'xlabel' : 'refresh ratio (%)'}]

