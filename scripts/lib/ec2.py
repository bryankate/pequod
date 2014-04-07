import sys
import os.path
from os import listdir, system, chmod
import stat
from time import sleep
import subprocess
from subprocess import Popen
import shlex
import json
import boto
from boto.ec2.connection import EC2Connection

REGION = 'us-east-1'

with open(os.path.join(os.path.dirname(os.path.realpath(__file__)), "aws.json")) as fp:
    cred = json.load(fp)
    AWS_ACCESS_KEY_ID = cred['AWS_ACCESS_KEY_ID']
    AWS_SECRET_ACCESS_KEY = cred['AWS_SECRET_ACCESS_KEY']

KEY_NAME = "pequod-" + REGION
SSH_KEY = os.path.join(os.path.dirname(os.path.realpath(__file__)), KEY_NAME + ".pem")

# note: instance have 10Gb ethernet only if launched in the same placement group
INSTANCE_TYPES = {'m1.small':    {'bid': 0.06, 'hvm': False},  # use for script testing
                  'cc2.8xlarge': {'bid': 2.41, 'hvm': True},   # 32 cores, 60.5GB RAM, 10Gb ethernet
                  'cr1.8xlarge': {'bid': 3.51, 'hvm': True},   # 32 cores, 244GB RAM, 10Gb ethernet
                  'hi1.4xlarge': {'bid': 0.76, 'hvm': True},   # 16 cores, 60.5GB RAM, 10Gb ethernet, SSDs
                  'hs1.8xlarge': {'bid': 0.76, 'hvm': True}}   # 16 cores, 117GB RAM, 10Gb ethernet

AMI_IDS = {'us-west-2': {'basic': 'ami-ccf297fc', 'hvm': 'ami-f8f297c8'},
           'us-east-1': {'basic': 'ami-bba18dd2', 'hvm': 'ami-e9a18d80'}}

INSTANCE_TYPE_BACKING = 'cr1.8xlarge'
INSTANCE_TYPE_CACHE = 'cr1.8xlarge'
INSTANCE_TYPE_CLIENT = 'cc2.8xlarge'


conn = None

def connect():
    print AWS_ACCESS_KEY_ID
    global conn
    if conn is None:
        conn = boto.ec2.connect_to_region(REGION, 
                                          aws_access_key_id=AWS_ACCESS_KEY_ID, 
                                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    chmod(SSH_KEY, stat.S_IREAD | stat.S_IWRITE)

def checkout_machines(num, type, spot=False):
    print "Checking out %s machines." % (num)
    ami_type = 'hvm' if INSTANCE_TYPES[type]['hvm'] else 'basic'
    reservation = conn.run_instances(AMI_IDS[REGION][ami_type],
                                     min_count = num, 
                                     max_count = num,
                                     key_name = KEY_NAME, 
                                     instance_type = type, 
                                     security_groups = ['pequod'],
                                     placement_group='pequod')
    return [i for i in reservation.instances]

def request_machines(num, type):
    print "Requesting %s spot instances." % (num)
    ami_type = 'hvm' if INSTANCE_TYPES[type]['hvm'] else 'basic'
    return conn.request_spot_instances(INSTANCE_TYPES[type]['bid'], 
                                       AMI_IDS[REGION][ami_type], 
                                       count = num,
                                       key_name = KEY_NAME, 
                                       instance_type = type, 
                                       security_groups = ['pequod'],
                                       placement_group='pequod',
                                       launch_group='pequod')

def startup_machines(instances):
    for i in instances:
        print "Starting instance %s" % (i.id)
        conn.start_instances([i.id])

def shutdown_machines(instances):
    for i in instances:
        if is_spot_instance(i):
            terminate_machines([i])
        else:
            print "Shutting down instance %s" % (i.id)
            conn.stop_instances([i.id], True)

def terminate_machines(instances):
    for i in instances:
        if (i.update(True) == "terminated"):
            continue
        
        print "Terminating instance %s" % (i.id)
        conn.terminate_instances([i.id])
        
        if is_spot_instance(i):
            conn.cancel_spot_instance_requests([i.spot_instance_request_id])
            
def get_all_instances():
    reservations = conn.get_all_instances()
    instances = [i for r in reservations for i in r.instances]
    return instances

def get_running_instances(type=None, tag=None):
    return filter_instances("running", type, tag)

def get_stopped_instances(type=None):
    return filter_instances("stopped", type, None)

def get_instances(instance_ids):
    ret = []
    all = get_all_instances()
    
    for i in instance_ids:
        for a in all:
            if a.id == i:
                ret.append(a)
    return ret

def filter_instances(status, type, tag):
    instances = get_all_instances()
    filtered = []
    for i in instances:
        if i.update(True) == status:
            if type and i.get_attribute('instanceType')['instanceType'] != type:
                continue
            if tag: 
                if tag[0] not in i.tags or i.tags[tag[0]] != tag[1]:
                    continue
            filtered.append(i)
    return filtered

def is_running(instance):
    return instance.update(True) == "running"

def is_spot_instance(instance):
    return instance.spot_instance_request_id

def get_all_spot_requests():
    return conn.get_all_spot_instance_requests()

def get_open_spot_requests():
    return filter_spot_requests('open')

def get_active_spot_requests():
    return filter_spot_requests('active')

def update_spot_requests(request_ids):
    return conn.get_all_spot_instance_requests(request_ids)

def filter_spot_requests(state):
    spots = []
    all = conn.get_all_spot_instance_requests()
    for r in all:
        if r.state == state:
            spots.append(r)
    return spots

def get_all_spot_instances():
    spots = []
    for i in get_all_instances():
        if is_spot_instance(i):
            spots.append(i)
    return spots

def cancel_spot_requests(requests):
    for r in requests:
        if r.state != "cancelled":
            print "Canceling spot request " + r.id
            conn.cancel_spot_instance_requests([r.id])
    
def scp_to(machine, tofile, fromfile):
    cmd = "scp -q -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s -r %s ec2-user@%s:%s" % \
          (SSH_KEY, fromfile, machine, tofile)
    Popen(cmd, shell=True).wait()

def scp_from(machine, fromfile, tofile):
    cmd = "scp -q -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s -r ec2-user@%s:%s %s" % \
          (SSH_KEY, machine, fromfile, tofile)
    Popen(cmd, shell=True).wait()

def run_ssh_command_bg(machine, cmd, tty=False):
    sshcmd = "ssh -A -q " + ("-t " if tty else "") + \
             "-o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -o ControlPath=none -i " + \
             "%s ec2-user@%s \"%s\"" % (SSH_KEY, machine, cmd)
    return Popen(shlex.split(sshcmd))

def run_ssh_command(machine, cmd, tty=False):
    return run_ssh_command_bg(machine, cmd, tty).wait()
