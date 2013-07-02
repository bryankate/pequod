# open loop test definitions
import copy

exps = [{'name': "twitter", 'defs': []}]
users = "--graph=twitter_graph_1.8M.dat"

serverCmd = "./obj/pqserver"
populateCmd = "./obj/pqserver --twitternew --verbose --no-execute %s" % (users)
clientCmd = "./obj/pqserver --twitternew --verbose --no-populate %s --duration=1000000 --popduration=0" % (users)

exps[0]['defs'].append(
    {'name': "autopush",
     'def_part': "twitternew",
     'backendcmd': "%s --berkeleydb" % (serverCmd),
     'cachecmd': serverCmd,
     'populatecmd': populateCmd,
     'clientcmd': "%s" % (clientCmd)})
