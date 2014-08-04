# list of experiments exported to calling script
exps = []

# add experiments to the global list.
# a local function that keeps the internal variables from becoming global
def define_experiments():
    binary = True
    binaryflag = "" if binary else "--no-binary"
    partfunc = "twitternew" if binary else "twitternew-text"
    fetch = "--fetch"

    serverCmd = "./obj/pqserver"
    appCmd = "./obj/pqserver --twitternew --verbose"
    clientCmd = "%s %s %s" % (appCmd, binaryflag, fetch)

    # remote eviction experiment
    # run on a real network using a two-tier deployment (1 cache, 1 backing, 1 client).
    exp = {'name': "evict-policy", 'defs': []}
    users = "--graph=twitter_graph_1.8M.dat"
    evict = "--evict-periodic --mem-lo=20000 --mem-hi=22500"
    clientBase = "%s %s --popduration=0 --duration=1000000000 --checklimit=62795845 " \
                 "--pactive=70 --ppost=1 --pread=100 --psubscribe=0 --plogout=0 " \
                 "--prevalidate" % (clientCmd, users)
    
    exp['defs'].append(
        {'name': "no-evict",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s %s" % (serverCmd),
         'clientcmd': "%s" % (clientBase)})

    '''
    exp['defs'].append(
        {'name': "rand-tomb",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s %s --evict-rand --evict-tomb" % (serverCmd, evict),
         'clientcmd': "%s" % (clientBase)})

    exp['defs'].append(
        {'name': "rand-notomb",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s %s --evict-rand --no-evict-tomb" % (serverCmd, evict),
         'clientcmd': "%s" % (clientBase)})
    '''

    exp['defs'].append(
        {'name': "lru-tomb",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s %s --no-evict-multi --evict-tomb" % (serverCmd, evict),
         'clientcmd': "%s" % (clientBase)})

    exp['defs'].append(
        {'name': "lru-notomb",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s %s --no-evict-multi --no-evict-tomb" % (serverCmd, evict),
         'clientcmd': "%s" % (clientBase)})

    exp['defs'].append(
        {'name': "multi-sink-tomb",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s %s --evict-multi --evict-tomb --evict-pref-sink" % (serverCmd, evict),
         'clientcmd': "%s" % (clientBase)})

    exp['defs'].append(
        {'name': "multi-sink-notomb",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s %s --evict-multi --no-evict-tomb --evict-pref-sink" % (serverCmd, evict),
         'clientcmd': "%s" % (clientBase)})

    exp['defs'].append(
        {'name': "multi-remote-tomb",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s %s --evict-multi --evict-tomb --no-evict-pref-sink" % (serverCmd, evict),
         'clientcmd': "%s" % (clientBase)})

    exp['defs'].append(
        {'name': "multi-remote-notomb",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s %s --evict-multi --no-evict-tomb --no-evict-pref-sink" % (serverCmd, evict),
         'clientcmd': "%s" % (clientBase)})
    exps.append(exp)

define_experiments()
