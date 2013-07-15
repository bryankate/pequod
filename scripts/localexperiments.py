exps = [{'name': "twitter", 'defs': []}]
users = "--nusers=1000"
binary = False
binaryflag = "" if binary else "--no-binary"
partfunc = "twitternew" if binary else "twitternew-text"

serverCmd = "./obj/pqserver"
initCmd = "./obj/pqserver --twitternew --verbose %s --initialize --no-populate --no-execute" % (binaryflag)
populateCmd = "./obj/pqserver --twitternew --verbose %s --no-initialize --no-execute %s" % (binaryflag, users)
clientCmd = "./obj/pqserver --twitternew --verbose %s --no-initialize --no-populate %s --duration=100000 --popduration=0" % (binaryflag, users)

exps[0]['defs'].append(
    {'name': "autopush",
     'def_part': partfunc,
     'def_db_type': "postgres",
     'def_db_writearound': True,
#     'backendcmd': "%s --evict-periodic --mem-lo=20 --mem-hi=25" % (serverCmd),
     'backendcmd': "%s" % (serverCmd),
     'cachecmd': serverCmd,
     'initcmd': initCmd,
     'populatecmd': populateCmd,
     'clientcmd': "%s" % (clientCmd)})
