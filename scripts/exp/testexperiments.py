# list of experiments exported to calling script
exps = []

# add experiments to the global list.
# a local function that keeps the internal variables from becoming global
def define_experiments():
    binary = False
    binaryflag = "" if binary else "--no-binary"
    partfunc = "twitternew" if binary else "twitternew-text"
    
    serverCmd = "./obj/pqserver --block-report=10"
    initCmd = "%s --twitternew --verbose %s --initialize --no-populate --no-execute" % (serverCmd, binaryflag)
    populateCmd = "%s --twitternew --verbose %s --no-initialize --no-execute --popduration=0" % (serverCmd, binaryflag)
    clientCmd = "%s --twitternew --verbose %s --no-initialize --no-populate" % (serverCmd, binaryflag)
    
    users = "--nusers=1000"
    clientBase = "%s %s --duration=100000" % (clientCmd, users)
    
    postgresPopulateCmd = "%s --twitternew --verbose --dbshim %s --no-execute --popduration=0 %s" % \
                          (serverCmd, binaryflag, users)
    postgresClientCmd ="%s --twitternew --verbose --dbshim %s --no-populate %s --duration=100000 " \
                       "--psubscribe=0 --plogout=0" % (serverCmd, binaryflag, users) 

    exps.append({'name': "basic", 
                 'defs': [{'def_part': partfunc,
                           'backendcmd': "%s" % (serverCmd),
                           'cachecmd': "%s" % (serverCmd),
                           'initcmd': "%s" % (initCmd),
                           'populatecmd': "%s %s" % (populateCmd, users),
                           'clientcmd': "%s --fetch" % (clientBase)}]})
    
    exps.append({'name': "writearound", 
                 'defs': [{'def_part': partfunc,
                           'def_db_type': "postgres",
                           'def_db_writearound': True,
                           'def_db_flags': "-c synchronous_commit=off -c fsync=off",
                           'def_db_sql_script': "scripts/exp/cache.sql",
                           'backendcmd': "%s" % (serverCmd),
                           'cachecmd': "%s" % (serverCmd),
                           'initcmd': "%s" % (initCmd),
                           'populatecmd': "%s %s --dbpool-max=5 --dbpool-depth=10" % (populateCmd, users),
                           'clientcmd': "%s --dbpool-max=5 --dbpool-depth=10" % (clientBase)}]})
    
    exps.append({'name': "postgres", 
                 'defs': [{'def_part': partfunc,
                           'def_db_type': "postgres",
                           #'def_db_in_memory': True,
                           'def_db_compare': True,
                           'def_db_sql_script': "scripts/exp/twitter-pg-schema.sql",
                           'def_db_flags': "-c synchronous_commit=off -c fsync=off " + \
                                           "-c full_page_writes=off  -c bgwriter_lru_maxpages=0 " + \
                                           "-c bgwriter_delay=10000 -c checkpoint_segments=600",
                           'populatecmd': "%s --dbpool-max=5 --dbpool-depth=100" % (postgresPopulateCmd),
                           'clientcmd': "%s --dbpool-depth=100" % (postgresClientCmd)}]})
    
    exps.append({'name': "redis", 
                 'defs': [{'def_redis_compare': True,
                           'populatecmd': "%s %s --push --redis" % (populateCmd, users),
                           'clientcmd': "%s --push --redis" % (clientBase)}]})
    
    exps.append({'name': "memcache", 
                 'defs': [{'def_memcache_compare': True,
                           'def_memcache_args': "-m 1024 -M",
                           'populatecmd': "%s %s --push --memcached" % (populateCmd, users),
                           'clientcmd': "%s --push --memcached" % (clientBase)}]})
    
    exps.append({'name': "eviction", 
                 'defs': [{'def_part': partfunc,
                           'def_db_type': "postgres",
                           'def_db_writearound': True,
                           'def_db_sql_script': "scripts/exp/cache.sql",
                           'backendcmd': "%s --evict-periodic --mem-lo=20 --mem-hi=25" % (serverCmd),
                           'cachecmd': "%s --evict-periodic --mem-lo=15 --mem-hi=20" % (serverCmd),
                           'initcmd': "%s" % (initCmd),
                           'populatecmd': "%s %s" % (populateCmd, users),
                           'clientcmd': "%s" % (clientBase)}]})

define_experiments()
