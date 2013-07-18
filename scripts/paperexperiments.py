# list of experiments exported to calling script
exps = []

# add experiments to the global list.
# a local function that keeps the internal variables from becoming global
def define_experiments():
    binary = True
    binaryflag = "" if binary else "--no-binary"
    partfunc = "twitternew" if binary else "twitternew-text"
    
    serverCmd = "./obj/pqserver"
    initCmd = "%s --twitternew --verbose %s --initialize --no-populate --no-execute" % (serverCmd, binaryflag)
    populateCmd = "%s --twitternew --verbose %s --no-initialize --no-execute" % (serverCmd, binaryflag)
    clientCmd = "%s --twitternew --verbose %s --no-initialize --no-populate" % (serverCmd, binaryflag)

    # policy experiment
    exp = {'name': "policy", 'defs': []}
    for active in [1, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]:
        users = "--nusers=twitter_graph_1.8M.dat"
        popBase = "%s --popduration=0"
        clientBase = "%s %s --pactive=%d --psubscribe=0 --plogout=0 --plogin=0 --duration=10000000" % (clientCmd, users, active)
        
        exp['defs'].append(
            {'name': "hybrid_%d" % (active),
             'def_part': partfunc,
             'backendcmd': "%s" % (serverCmd),
             'cachecmd': "%s" % (serverCmd),
             'initcmd': "%s" % (initCmd),
             'populatecmd': "%s %s" % (popBase, users),
             'clientcmd': "%s" % (clientBase)})
    
        exp['defs'].append(
            {'name': "pull_%s" % (str(active)),
             'def_part': partfunc,
             'backendcmd': "%s" % (serverCmd),
             'cachecmd': "%s" % (serverCmd),
             'initcmd': "%s --pull" % (initCmd),
             'populatecmd': "%s %s --pull" % (popBase, users),
             'clientcmd': "%s --pull" % (clientBase)})
        
        exp['defs'].append(
            {'name': "push_%s" % (str(active)),
             'def_part': partfunc,
             'backendcmd': "%s" % (serverCmd),
             'cachecmd': "%s" % (serverCmd),
             'initcmd': "%s --push" % (initCmd),
             'populatecmd': "%s %s --push" % (popBase, users),
             'clientcmd': "%s --push" % (clientBase)})
    exps.append(exp)

define_experiments()
