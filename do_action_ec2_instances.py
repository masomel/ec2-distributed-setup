'''
Apply a given command to all existing EC2 instances

author: Marcela S. Melara (melara@cs.princeton.edu)
date created: 02/04/2016
'''

import sys

# external imports
from boto import ec2
from glob import glob

# Stormship imports
import ec2_util

# print usage message
def usage():
    print('Usage: python3 do_action_ec2_instances.py <action> [cmd with args | from path] [to path]')
    print('Supported actions: start, stop, cmd, scp_get, scp_put')
    exit()

## Main script ##
if len(sys.argv) < 2:
    usage()

action = sys.argv[1]
path = ''
nodes = ec2_util.read_ec2_node_list(path)

if action != 'start' and action != 'stop' and action != 'cmd' and action != 'scp_get' and action != 'scp_put':
    usage()

for node, node_code in nodes.items():
    # connect to the ec2 instance
    (conn, instance) = ec2_util.connect_to_ec2_instance(node)

    if action == 'start':
        print('Starting '+node_code)
        instance = ec2_util.start_ec2_instance(conn, instance)
    elif action == 'stop':
        print('Stopped '+node_code)
        instance = ec2_util.stop_ec2_instance(conn, instance)
    elif action == 'cmd' or action == 'scp_get' or action == 'scp_put':
        # start the instance if it isn't running
        if instance.state != 'running':
            print('Started '+node_code)
            instance = ec2_util.start_ec2_instance(conn, instance)
        
        # run the given command
        if action == 'cmd':
            cmd = sys.argv[2]
            print('Running command on '+node_code)
            ec2_util.run_cmd_on_ec2_instance(instance, cmd)
        elif action == 'scp_get' or action == 'scp_put':
            from_path = sys.argv[2]
            to_path = sys.argv[3]
            if action == 'scp_get':
                if '*' in from_path:
                    from_path = from_path.replace("*", node_code)
                print('Getting files from '+node_code)
                ec2_util.scp_from_ec2_instance(instance, path, from_path, to_path)
            elif action == 'scp_put':
                from_path = glob(path+'/'+from_path)
                print('Sending files to '+node_code)
                ec2_util.scp_to_ec2_instance(instance, from_path, to_path)
                


