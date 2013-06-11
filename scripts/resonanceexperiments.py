# open loop test definitions
import copy

exps = [{'name': "twitter", 'defs': []}]

serverCmd = "./obj/pqserver"
clientCmd = "./obj/pqserver --twitternew --plogout=0 --psubscribe=0 --duration=10000000 --popduration=0 --graph=twitter_graph_22M.dat"

exps[0]['defs'].append(
    {'name': "autopush",
     'def_part': "twitternew",
     'servercmd': serverCmd,
     'clientcmd': "%s" % (clientCmd)})

'''
exps[0]['defs'].append(
    {'name': "push",
     'def_part': "twitternew",
     'servercmd': serverCmd,
     'clientcmd': "%s --push" % (clientCmd)})
'''
