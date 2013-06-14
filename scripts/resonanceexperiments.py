# open loop test definitions
import copy

exps = [{'name': "twitter", 'defs': []}]
graphfile = "twitter_graph_40M.dat"

serverCmd   = "./obj/pqserver"
populateCmd = "./obj/pqserver --verbose --twitternew --no-execute --graph=%s" % (graphfile)
clientCmd   = "./obj/pqserver --verbose --twitternew --no-populate --plogout=0 --psubscribe=0 --duration=400000000 --popduration=0 --graph=%s" % (graphfile)


exps[0]['defs'].append(
    {'name': "autopush",
     'def_part': "twitternew",
     'servercmd': serverCmd,
     'populatecmd': populateCmd,
     'clientcmd': clientCmd})

'''
exps[0]['defs'].append(
    {'name': "push",
     'def_part': "twitternew",
     'servercmd': serverCmd,
     'populatecmd': populateCmd,
     'clientcmd': "%s --push" % (clientCmd)})
'''
