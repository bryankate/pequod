import sys
import os.path
from os import listdir, system, chmod
import stat
from time import sleep
import subprocess
from subprocess import Popen
import boto
from boto.ec2.connection import EC2Connection

REGION = 'us-west-2'
AVAILABILITY_ZONE = 'us-west-2b'

AWS_ACCESS_KEY_ID = 'AKIAJSSPS6LP2VMU4WUA'
AWS_SECRET_ACCESS_KEY = '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'
KEY_NAME = "pequod-" + REGION
SSH_KEY = os.path.join(os.path.dirname(os.path.realpath(__file__)), KEY_NAME + ".pem")

AMI_IDS = {'us-east-1': 'ami-c30360aa',
           'us-west-2': 'ami-bf1d8a8f'}

INSTANCE_TYPE_BACKING = 'm1.small'
INSTANCE_TYPE_CACHE = 'm1.small'
INSTANCE_TYPE_CLIENT = 'm1.small'

SPOT_BIDS = {'m1.small':    0.06,
             'm1.xlarge':   0.50,
             'm3.2xlarge':  0.85,
             'c1.xlarge':   0.60,
             'm2.4xlarge':  1.65,
             'cc2.8xlarge': 2.50,
             'cr1.8xlarge': 3.50, 
             'hi1.4xlarge': 3.10, 
             'hs1.8xlarge': 4.60}

conn = None

def connect():
    global conn
    if conn is None:
        conn = boto.ec2.connect_to_region(REGION, 
                                          aws_access_key_id=AWS_ACCESS_KEY_ID, 
                                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
    chmod(SSH_KEY, stat.S_IREAD | stat.S_IWRITE)

def checkout_machines(num, type, spot=False):
    print "Checking out %s machines." % (num)
    reservation = conn.run_instances(AMI_IDS[REGION],
                                     min_count = num, 
                                     max_count = num,
                                     key_name = KEY_NAME, 
                                     instance_type = type, 
                                     security_groups = ['pequod'],
                                     placement = AVAILABILITY_ZONE)
    return [i for i in reservation.instances]

def request_machines(num, type):
    print "Requesting %s spot instances." % (num)
    return conn.request_spot_instances(SPOT_BIDS[type], 
                                       AMI_IDS[REGION], 
                                       count = num,
                                       key_name = KEY_NAME, 
                                       instance_type = type, 
                                       security_groups = ['pequod'],
                                       placement = AVAILABILITY_ZONE)

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
    Popen("scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s -r %s ubuntu@%s:%s" % (SSH_KEY, fromfile, machine, tofile),
          shell=True).wait()

def scp_from(machine, fromfile, tofile):
    Popen("scp -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s -r ubuntu@%s:%s %s" % (SSH_KEY, machine, fromfile, tofile),
          shell=True).wait()

def run_ssh_command_bg(machine, cmd):
    return Popen("ssh -A -q -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no -i %s ubuntu@%s \"%s\"" % (SSH_KEY, machine, cmd),
                 shell=True)

def run_ssh_command(machine, cmd):
    return run_ssh_command_bg(machine, cmd).wait()
    
def test_ssh_connection(machine):
    return run_ssh_command(machine, "echo 2>&1 \"%s is Live\"" % (machine)) == 0
