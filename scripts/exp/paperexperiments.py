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
    clientCmd = "%s %s --fetch --no-initialize --no-populate " % (appCmd, binaryflag)

    # policy experiment
    # can be run on on a multiprocessor
    #
    # we set the number of operations so that there are 
    # about 140 timeline checks per active user.
    # the number of posts is fixed, so there ends up being a variable
    # post:check ratio of 1:1 at 1% active users to 1:100 at 100% active users
    exp = {'name': "policy", 'defs': []}
    users = "--graph=twitter_graph_1.8M.dat"
    
    points = [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]
    for active in points:
        popBase = "%s %s --popduration=0" % (populateCmd, users)
        clientBase = "%s %s --pactive=%d --duration=1000000000 --postlimit=2511834 " \
                     "--ppost=1 --pread=%d --psubscribe=0 --plogin=0 --plogout=0" % \
                     (clientCmd, users, active, active)
        
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
    
    exp['plot'] = {'type': "line",
                   'data': [{'from': "client",
                             'attr': "wall_time"}],
                   'lines': ["hybrid", "pull", "push"],
                   'points': points,
                   'xlabel': "Percent Active",
                   'ylabel': "Runtime (s)"}
    exps.append(exp)
    
    # read-write ratio experiment
    # can be run on on a multiprocessor
    #
    # fix the %active at 70 and have each user do 140 timeline checks.
    # vary the number of posts that are made so that the post:check ratio
    # goes from 1:100 to 2:1
    exp = {'name': "writeratio", 'defs': []}
    users = "--graph=twitter_graph_1.8M.dat"
    
    points = [1, 25, 50, 75, 100, 200]
    for post in points:
        popBase = "%s %s --popduration=0" % (populateCmd, users)
        clientBase = "%s %s --pactive=70 --duration=1000000000 --checklimit=175828366 " \
                     "--ppost=%d --pread=100 --psubscribe=0 --plogin=0 --plogout=0" % \
                     (clientCmd, users, post)
        
        exp['defs'].append(
            {'name': "hybrid_%d" % (post),
             'def_part': partfunc,
             'backendcmd': "%s" % (serverCmd),
             'cachecmd': "%s" % (serverCmd),
             'initcmd': "%s" % (initCmd),
             'populatecmd': "%s" % (popBase),
             'clientcmd': "%s --no-prevalidate" % (clientBase)})
    
        exp['defs'].append(
            {'name': "push_%d" % (post),
             'def_part': partfunc,
             'backendcmd': "%s" % (serverCmd),
             'cachecmd': "%s" % (serverCmd),
             'initcmd': "%s" % (initCmd),
             'populatecmd': "%s" % (popBase),
             'clientcmd': "%s --prevalidate --prevalidate-inactive" % (clientBase)})
        
        exp['defs'].append(
            {'name': "push_update_%d" % (post),
             'def_part': partfunc,
             'backendcmd': "%s" % (serverCmd),
             'cachecmd': "%s" % (serverCmd),
             'initcmd': "%s" % (initCmd),
             'populatecmd': "%s --pactive=70 --prevalidate-before-sub --prevalidate --prevalidate-inactive" % (popBase),
             'clientcmd': "%s" % (clientBase)})
    
    exp['plot'] = {'type': "line",
                   'data': [{'from': "client",
                             'attr': "wall_time"}],
                   'lines': ["hybrid", "push"],
                   'points': points,
                   'xlabel': "Percent Active",
                   'ylabel': "Runtime (s)"}
    exps.append(exp)
    
    
    # client push vs. pequod experiment
    # can be run on a multiprocessor
    #
    # fix the %active at 70 and have each user perform 140 timeline checks.
    # fix the post:check ratio at 1:100 
    exp = {'name': "client_push", 'defs': []}
    users = "--graph=twitter_graph_1.8M.dat"
    popBase = "%s %s --popduration=0" % (populateCmd, users)
    clientBase = "%s %s --pactive=70 --duration=1000000000 --checklimit=175828366 " \
                 "--ppost=1 --pread=100 --psubscribe=10 --plogin=5 --plogout=5" % \
                 (clientCmd, users)
    
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
    
    exp['plot'] = {'type': "stackedbar",
                   'data': [{'from': "server",
                             'attr': "server_wall_time_insert"},
                            {'from': "server",
                             'attr': "server_wall_time_validate"},
                            {'from': "server",
                             'attr': "server_wall_time_other"}],
                   'lines': ["pequod", "pequod_warm", "client_push"],
                   'ylabel': "Runtime (s)"}
    exps.append(exp)


    # pequod optimization factor analysis experiment.
    # this test will need to be run multiple times against different builds.
    # specifically, the code needs to be reconfigured with --disable-hint and 
    # --disable-value-sharing to produce the results for the different factors.
    # can be run on a multiprocessor
    exp = {'name': "optimization", 'defs': []}
    users = "--graph=twitter_graph_1.8M.dat"
    buildBase = "./configure --with-malloc=jemalloc"
    clientBase = "%s %s --pactive=70 --duration=1000000000 --checklimit=175828366 " \
                 "--ppost=1 --pread=100 --psubscribe=10 --plogin=5 --plogout=5" % \
                 (clientCmd, users)
    
    exp['defs'].append(
        {'name': "pequod-base",
         'def_part': partfunc,
         'def_build': "%s --disable-hint --disable-value-sharing; make" % (buildBase),
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s" % (initCmd),
         'populatecmd': "%s %s --popduration=0" % (populateCmd, users),
         'clientcmd': "%s" % (clientBase)})
    
    exp['defs'].append(
        {'name': "pequod-hint",
         'def_part': partfunc,
         'def_build': "%s --disable-value-sharing; make" % (buildBase),
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s" % (initCmd),
         'populatecmd': "%s %s --popduration=0" % (populateCmd, users),
         'clientcmd': "%s" % (clientBase)})
        
    exp['defs'].append(
        {'name': "pequod-hint-share",
         'def_part': partfunc,
         'def_build': "%s; make" % (buildBase),
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s" % (initCmd),
         'populatecmd': "%s %s --popduration=0" % (populateCmd, users),
         'clientcmd': "%s" % (clientBase)})
    
    exp['plot'] = {'type': "stackedbar",
                   'data': [{'from': "server",
                             'attr': "server_wall_time_insert"},
                            {'from': "server",
                             'attr': "server_wall_time_validate"},
                            {'from': "server",
                             'attr': "server_wall_time_other"}],
                   'lines': ["pequod-base, pequod-hint, pequod-hint-share"],
                   'ylabel': "Runtime (s)"}
    exps.append(exp)
    
    # computation experiment. 
    # measure computation times inside the server and see that they are small.
    # should be run both on a multiprocessor and in a distributed setup 
    # with one cache server and one or more backing servers that own all the base data.
    #
    # the enable_validation_logging flag should be set to true and the 
    # logs should be used to determine avg, stddev, and percentiles for
    # the computation times.
    exp = {'name': "computation", 'defs': []}
    users = "--graph=twitter_graph_1.8M.dat"
    popBase = "%s %s --popduration=1000000" % (populateCmd, users),
    clientBase = "%s %s --no-prevalidate --pactive=70 --duration=1000000000 --checklimit=175828366 " \
                 "--ppost=1 --pread=100 --psubscribe=10 --plogin=5 --plogout=5" % \
                 (clientCmd, users)
    
    exp['defs'].append(
        {'name': "pequod",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s" % (initCmd),
         'populatecmd': "%s" % (popBase),
         'clientcmd': "%s" % (clientBase)})
    exps.append(exp)
    
    # database as cache comparison experiment.
    # can be run on a multiprocessor.
    #
    # the number of cache servers pequod uses should be the same as the number of 
    # clients used to access postgres through the DBPool.
    # fix %active at 70, post:check ratio at 1:100 and 140 timeline checks per user. 
    exp = {'name': "dbcompare", 'defs': []}
    users = "--graph=twitter_graph_1.8M.dat"
    clientBase = "%s %s --pactive=70 --duration=1000000000 --checklimit=175828366 " \
                 "--ppost=1 --pread=100 --psubscribe=10 --plogin=5 --plogout=5" % \
                 (clientCmd, users)
    
    exp['defs'].append(
        {'name': "pequod",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s" % (serverCmd),
         'initcmd': "%s" % (initCmd),
         'populatecmd': "%s %s --popduration=0" % (populateCmd, users),
         'clientcmd': "%s" % (clientBase)})
    
    exp['defs'].append(
        {'name': "postgres",
         'def_db_type': "postgres",
         'def_db_sql_script': "scripts/exp/twitter-pg-schema.sql",
         'def_db_in_memory': True,
         'def_db_compare': True,
         'populatecmd': "%s %s --dbshim --initialize --popduration=0" % (populateCmd, users),
         'clientcmd': "%s --dbshim --initialize" % (clientBase)})
    
    exp['plot'] = {'type': "bar",
                   'data': [{'from': "client",
                             'attr': "wall_time"}],
                   'lines': ["pequod", "postgres"],
                   'ylabel': "Runtime (s)"}
    exps.append(exp)

define_experiments()
