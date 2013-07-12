import copy

exps = [{'name': "twitter", 'defs': []}]
users = "--nusers=1000"

serverCmd = "./obj/pqserver"
populateCmd = "./obj/pqserver --twitternew --no-binary --verbose --no-execute %s" % (users)
clientCmd = "./obj/pqserver --twitternew --no-binary --verbose --no-populate %s --duration=10000 --popduration=0" % (users)

exps[0]['defs'].append(
    {'name': "autopush",
     'def_part': "twitternew-text",
#     'def_db_type': "postgres",
#     'def_db_writearound': True,
     'backendcmd': "%s" % (serverCmd),
     'cachecmd': serverCmd,
     'populatecmd': populateCmd,
     'clientcmd': "%s" % (clientCmd)})
