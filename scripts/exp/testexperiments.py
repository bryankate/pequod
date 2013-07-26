# list of experiments exported to calling script
exps = []

# add experiments to the global list.
# a local function that keeps the internal variables from becoming global
def define_experiments():
    binary = False
    binaryflag = "" if binary else "--no-binary"
    partfunc = "twitternew" if binary else "twitternew-text"
    
    serverCmd = "./obj/pqserver"
    initCmd = "%s --twitternew --verbose %s --initialize --no-populate --no-execute" % (serverCmd, binaryflag)
    populateCmd = "%s --twitternew --verbose %s --no-initialize --no-execute --popduration=0" % (serverCmd, binaryflag)
    clientCmd = "%s --twitternew --verbose %s --no-initialize --no-populate" % (serverCmd, binaryflag)
    
    users = "--nusers=1000"
    clientBase = "%s %s --duration=100000" % (clientCmd, users)
    
    exps.append({'name': "basic", 
                 'defs': [{'def_part': partfunc,
                           'backendcmd': "%s" % (serverCmd),
                           'cachecmd': "%s" % (serverCmd),
                           'initcmd': "%s" % (initCmd),
                           'populatecmd': "%s %s" % (populateCmd, users),
                           'clientcmd': "%s" % (clientBase)}]})
    
    exps.append({'name': "writearound", 
                 'defs': [{'def_part': partfunc,
                           'def_db_type': "postgres",
                           'def_db_writearound': True,
                           'backendcmd': "%s" % (serverCmd),
                           'cachecmd': "%s" % (serverCmd),
                           'initcmd': "%s" % (initCmd),
                           'populatecmd': "%s %s" % (populateCmd, users),
                           'clientcmd': "%s" % (clientBase)}]})
    
    exps.append({'name': "eviction", 
                 'defs': [{'def_part': partfunc,
                           'def_db_type': "postgres",
                           'def_db_writearound': True,
                           'backendcmd': "%s --evict-periodic --mem-lo=20 --mem-hi=25" % (serverCmd),
                           'cachecmd': "%s --evict-periodic --mem-lo=15 --mem-hi=20" % (serverCmd),
                           'initcmd': "%s" % (initCmd),
                           'populatecmd': "%s %s" % (populateCmd, users),
                           'clientcmd': "%s" % (clientBase)}]})

define_experiments()
