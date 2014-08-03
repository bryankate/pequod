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
    initCmd = "%s %s --initialize --no-populate --no-execute" % (appCmd, binaryflag)
    clientCmd = "%s %s %s %s --no-initialize" % (appCmd, binaryflag, fetch, users)

    # remote eviction experiment
    # run on a real network using a two-tier deployment.
    # change compilation settings to test different policies
    # that prefer sink vs. remote range eviction, random eviction, etc.
    exp = {'name': "remote-eviction", 'defs': []}
    users = "--graph=twitter_graph_1.8M.dat"
    # evict = "--evict-periodic --mem-lo=250 --mem-hi=300"
    evict = ""
    clientBase = "%s %s --popduration=0 --duration=1000000000 --checklimit=62795845 " \
                 "--pactive=70 --ppost=1 --pread=100 --psubscribe=0 --plogout=0" % \
                 (clientCmd, users)
    
    exp['defs'].append(
        {'name': "evict",
         'def_part': partfunc,
         'backendcmd': "%s" % (serverCmd),
         'cachecmd': "%s %s" % (serverCmd, evict),
         'initcmd': "%s" % (initCmd),
         'clientcmd': "%s --prevalidate" % (clientBase)})
    exps.append(exp)

define_experiments()
