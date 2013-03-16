import getpass, os

sanityRun = False

# The user name and RSA key to connect to all other machines, including the git
# server, experiment controlling machine, servers and load generators
USER = getpass.getuser()

# SSH_KEYFILE must be None for now. The consequence is that you must enable ssh
# forwarding. To enable ssh forwarding, the simplest way is:
#   - create a public/private key pair.
#   - put the public key into ~/.ssh/authorized_keys on every every server you
#     want to ssh to.
#   - save the private key as ~/.ssh/id_rsa on your own laptop or desktop.
#   - run "ssh-add" in the shell of your own laptop or desktop. Note: if you
#     run experiemnts from a remote machine, you need to "ssh -A" to the machine,
#     or add "ForwardAgent yes" into ~/.ssh/config.
# Now ssh to any server, and you should be able to ssh to any other server without
# password!
SSH_KEYFILE = None

def ssh_command():
    return "ssh -i " + SSH_KEYFILE if SSH_KEYFILE != None else "ssh -A"
def scp_command():
    return "scp -i " + SSH_KEYFILE if SSH_KEYFILE != None else "scp"

# XXX: the proc, cores and cmdMod fields are ignored right now.
# In the future, they will have the following semantic:
#   proc: maximum number of processes that a client can run
#   cores: the cores that the load generating processes is allowed
#          to run on. The load generators are assigned to a core
#          round-robin.
#   cmdMod: command prefix on each command.

LOCAL_SERVER = [{'addr' : 'localhost', 'proc' : 100, 'cores' : 24}]
LOCAL_CLIENT = [{'addr' : 'localhost', 'proc' : 100, 'cores' : 24}]

BEN_SERVER = [{'addr' : 'ben.csail.mit.edu', 'proc' : 100, 'cores' : 100}]
BEN_CLIENT = [{'addr' : 'ben.csail.mit.edu', 'proc' : 100, 'cores' : 100}]

PDOS_SERVERS = [
    {'addr': "oc-5.csail.mit.edu", "proc" : 2, "cores" : 2},
    {'addr': "oc-6.csail.mit.edu", "proc" : 2, "cores" : 2},
    {'addr': "oc-7.csail.mit.edu", "proc" : 2, "cores" : 2},
    {'addr': "oc-8.csail.mit.edu", "proc" : 2, "cores" : 2},
    {'addr': "oc-9.csail.mit.edu", "proc" : 2, "cores" : 2},
    {'addr': "oc-10.csail.mit.edu", "proc" : 2, "cores" : 2},
    {'addr': "oc-11.csail.mit.edu", "proc" : 2, "cores" : 2}]

PDOS_CLIENTS = [ 
    {'addr': "mat.csail.mit.edu", "proc": 100, "cores" : 24},
    #{'addr': "ben.csail.mit.edu", "proc": 96, "cores" : 48, 
    #    'cmdMod': "/home/am6/mpdev/bin/perflock -s"},
    #{'addr': "josmp.csail.mit.edu", "proc": 48, "cores" : 16, 
    #    'cmdMod': "/home/am6/mpdev/bin/perflock -s"},
    #{'addr': "brick-1.csail.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "brick-2.csail.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "brick-3.csail.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "brick-4.csail.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "brick-5.lcs.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "brick-6.lcs.mit.edu", "proc" : 1, "cores" : 1}, # single core?
    # The disk of brick-7 is full.
    #{'addr': "brick-7.lcs.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "brick-8.lcs.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "brick-9.lcs.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "brick-10.lcs.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "brick-11.lcs.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "brick-12.lcs.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "brick-14.lcs.mit.edu", "proc" : 2, "cores" : 2},
    #{'addr': "ud0.csail.mit.edu", "proc" : 4, "cores" : 4},
    #{'addr': "ud1.csail.mit.edu", "proc" : 4, "cores" : 4},
    #{'addr': "hooverdam.lcs.mit.edu", "proc" : 4, "cores" : 4},
    #{'addr': "hydra.lcs.mit.edu", "proc" : 4, "cores" : 4},
]

PDOS_CLUSTER = {'servers' : PDOS_SERVERS, 'clients' : PDOS_CLIENTS}
LOCAL_CLUSTER = {'clients' : LOCAL_CLIENT, 'servers' : LOCAL_SERVER}
PWD_CLUSTER = {'clients' : LOCAL_CLIENT, 'servers' : LOCAL_SERVER}
BEN_CLUSTER = {'clients' : BEN_CLIENT, 'servers' : BEN_SERVER}
CLUSTERS = {'pdos' : PDOS_CLUSTER,
            'harvard' : None,
            'local' : LOCAL_CLUSTER,
            'pwd' : LOCAL_CLUSTER,
            'ben' : BEN_CLUSTER}
