# open loop test definitions
import copy

rwmicro = []
cmdbase = "./obj/pqserver --nusers=2000 --nops=2000000 --rwmicro --nfollower=20 "
prefresh = [0, 5, 10, 20, 40, 60, 80, 90, 95, 100]
for pr in prefresh:
    rwmicro.append(
        {'plotgroup': "push",
         'plotkey' : pr,
         'cmd': "%s --prefresh=%d --push" % (cmdbase, pr)})
    rwmicro.append(
        {'plotgroup': "pull",
         'plotkey' : pr,
         'cmd': "%s --prefresh=%d --no-push" % (cmdbase, pr)})


exps = [{'name': "rwmicro", 'defs': rwmicro, 'xlabel' : 'refresh ratio (%)'},
        ]

