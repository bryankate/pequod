# list of experiments exported to calling script
exps = []

# add experiments to the global list.
# a local function that keeps the internal variables from becoming global
def define_experiments():
    binary = True
    binaryflag = "" if binary else "--no-binary"
    partfunc = "twitternew" if binary else "twitternew-text"
    
    serverCmd = "./obj/pqserver"
    appCmd = "./obj/pqserver --twitternew --verbose"
    initCmd = "%s %s --initialize --no-populate --no-execute" % (appCmd, binaryflag)
    populateCmd = "%s %s --no-initialize --no-execute" % (appCmd, binaryflag)
    clientCmd = "%s %s --fetch --no-initialize --no-populate " % (appCmd, binaryflag) + \
                "--ppost=1 --psubscribe=60 --plogin=5 --plogout=5 --psubscribe=10"

    # policy experiment
    # can be run on on a multiprocessor
    exp = {'name': "policy", 'defs': []}
    users = "--nusers=twitter_graph_1.8M.dat"
    for active in [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        popBase = "%s %s --popduration=0" % (populateCmd, users)
        clientBase = "%s %s --pactive=%d --duration=50000000" % (clientCmd, users, active)
        
        exp['defs'].append(
            {'name': "hybrid_%d" % (active),
             'def_part': partfunc,
             'backendcmd': "%s" % (serverCmd),
             'cachecmd': "%s" % (serverCmd),
             'initcmd': "%s" % (initCmd),
             'populatecmd': "%s" % (popBase),
             'clientcmd': "%s --no-prevalidate" % (clientBase)})
    
        exp['defs'].append(
            {'name': "pull_%s" % (str(active)),
             'def_part': partfunc,
             'backendcmd': "%s" % (serverCmd),
             'cachecmd': "%s" % (serverCmd),
             'initcmd': "%s --pull" % (initCmd),
             'populatecmd': "%s" % (popBase),
             'clientcmd': "%s --pull" % (clientBase)})
        
        exp['defs'].append(
            {'name': "push_%s" % (str(active)),
             'def_part': partfunc,
             'backendcmd': "%s" % (serverCmd),
             'cachecmd': "%s" % (serverCmd),
             'initcmd': "%s" % (initCmd),
             'populatecmd': "%s" % (popBase),
             'clientcmd': "%s --prevalidate --prevalidate-inactive" % (clientBase)})
    exps.append(exp)
    
    
    # client push vs. pequod experiment
    # can be run on a multiprocessor
    exp = {'name': "client_push", 'defs': []}
    users = "--nusers=twitter_graph_1.8M.dat"
    popBase = "%s %s --popduration=0" % (populateCmd, users)
    clientBase = "%s %s --pactive=70 --duration=50000000" % (clientCmd, users)
    
    exp['defs'].append(
        {'name': "pequod",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s" % (initCmd),
         'populatecmd': "%s" % (popBase),
         'clientcmd': "%s --no-prevalidate" % (clientBase)})
    
    exp['defs'].append(
        {'name': "pequod_warm",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s" % (initCmd),
         'populatecmd': "%s" % (popBase),
         'clientcmd': "%s --prevalidate" % (clientBase)})
    
    exp['defs'].append(
        {'name': "client_push",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s --push" % (initCmd),
         'populatecmd': "%s --push" % (popBase),
         'clientcmd': "%s --push" % (clientBase)})
    exps.append(exp)


    # pequod optimization factor analysis experiment.
    # this test will need to be run multiple times against different builds.
    # specifically, the code needs to be reconfigured with --disable-hint and 
    # --disable-value-sharing to produce the results for the different factors.
    # can be run on a multiprocessor
    exp = {'name': "optimization", 'defs': []}
    users = "--nusers=twitter_graph_1.8M.dat"
    
    exp['defs'].append(
        {'name': "pequod",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s" % (initCmd),
         'populatecmd': "%s %s --popduration=0" % (populateCmd, users),
         'clientcmd': "%s %s --pactive=70 --duration=50000000" % (clientCmd, users)})
    exps.append(exp)
    
    # computation experiment. 
    # measure computation times inside the server and see that they are small.
    # should be run both on a multiprocessor and in a distributed setup 
    # with one cache server and one or more backing servers that own all the base data
    exp = {'name': "computation", 'defs': []}
    users = "--nusers=twitter_graph_1.8M.dat"
    popBase = "%s %s --popduration=100000" % (populateCmd, users),
    clientBase = "%s %s --no-prevalidate --pactive=70 --duration=50000000" % (clientCmd, users)
    
    exp['defs'].append(
        {'name': "pequod",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s" % (initCmd),
         'populatecmd': "%s" % (popBase),
         'clientcmd': "%s" % (clientBase)})
    
    exp['defs'].append(
        {'name': "pequod_sync",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s" % (initCmd),
         'populatecmd': "%s" % (popBase),
         'clientcmd': "%s --synchronous" % (clientBase)})
    exps.append(exp)

define_experiments()
