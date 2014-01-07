# list of experiments exported to calling script
exps = []

# add experiments to the global list.
# a local function that keeps the internal variables from becoming global
def define_experiments():
    serverCmd = "./obj/pqserver"
    popCmd = "./obj/poptable"
    clientCmd = "./obj/memtier_benchmark"
    
    exp = {'name': "logn", 'defs': []}
    sizes = [100, 1000, 10000, 100000, 1000000, 10000000, 100000000]
    popBase = "%s --padding=10" % (popCmd)
    clientBase = "%s --ratio 0:1 --key-padding 10" % (clientCmd)
    
    for s in sizes:
        exp['defs'].append(
            {'name': "pequod_%d" % s,
             'cachecmd': "%s" % (serverCmd),
             'populatecmd': "%s --maxkey=%d" % (popBase, s),
             'clientcmd': "%s --key-maximum %d -P pequod" % (clientBase, s)})
        
        exp['defs'].append(
            {'name': "redis_%d" % s,
             'def_redis_compare': True,
             'populatecmd': "%s --maxkey=%d --redis" % (popBase, s),
             'clientcmd': "%s --key-maximum %d -P redis" % (clientBase, s)})
        
        exp['defs'].append(
            {'name': "memcache_%d" % s,
             'def_memcache_compare': True,
             'def_memcache_args': "-m 2048 -M",
             'populatecmd': "%s --maxkey=%d --memcached" % (popBase, s),
             'clientcmd': "%s --key-maximum %d -P memcache_binary" % (clientBase, s)})
        
    exps.append(exp)

define_experiments()
