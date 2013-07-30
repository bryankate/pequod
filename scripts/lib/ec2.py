import sys
import os.path
from os import listdir, system
from time import sleep
import subprocess
from subprocess import Popen
import boto
from boto.ec2.connection import EC2Connection

AWS_ACCESS_KEY_ID = 'AKIAJSSPS6LP2VMU4WUA'
AWS_SECRET_ACCESS_KEY = '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'
SSH_KEY="scripts/pequod.pem"

AMI_ID = 'ami-c30360aa'
INSTANCE_TYPE_BACKING = 't1.micro'
INSTANCE_TYPE_CACHE = 't1.micro'
INSTANCE_TYPE_CLIENT = 't1.micro'

conn = None

def connect():
    global conn
    if conn is None:
        conn = EC2Connection(AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY)

def checkout_machines(num, type):
    print "Checking out %s machines." % (num)
    reservation = conn.run_instances(AMI_ID, 
                                     min_count=num, max_count=num,
                                     key_name='pequod', 
                                     instance_type=type, 
                                     security_groups=['pequod'])
    return [i for i in reservation.instances]

def startup_machines(instances):
    for i in instances:
        print "Starting instance %s" % (i.id)
        conn.start_instances([i.id])

def shutdown_machines(instances):
    for i in instances:
        print "Shutting down instance %s" % (i.id)
        conn.stop_instances([i.id], True)

def terminate_machines(instances):
    for i in instances:
        print "Terminating instance %s" % (i.id)
        conn.stop_instances([i.id])

def get_all_instances():
    reservations = conn.get_all_instances()
    instances = [i for r in reservations for i in r.instances]
    return instances

def get_running_instances(type=None, tag=None):
    return filter_instances("running", type, tag)

def get_stopped_instances(type=None):
    return filter_instances("stopped", type, None)

def filter_instances(status, type, tag):
    instances = get_all_instances()
    filtered = []
    for i in instances:
        if i.update() == status:
            if type and i.get_attribute('instanceType')['instanceType'] != type:
                continue
            if tag: 
                if tag[0] not in i.tags or i.tags[tag[0]] != tag[1]:
                    continue
            filtered.append(i)
    return filtered

def is_running(instance):
    return instance.update() == "running"

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
