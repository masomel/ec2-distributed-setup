'''
Utility functions for building and running distributed EC2 jobs.
The list of EC2 nodes used is in ec2_node_list.txt

author: Marcela S. Melara (melara@cs.princeton.edu)
date created: 02/04/2016
'''
from boto import ec2 # use boto 2, the API is better

from collections import OrderedDict
from subprocess import call
import time

# reads in all node hostnames
# from the file located in the path <path_to_list>
# returns an ordered dictionary mapping each node's hostname to its code
def read_ec2_node_list(path_to_list, node_list):
    nodes = OrderedDict() # Want to map node hostnames to codes for abbreviated printing
    f = open(path_to_list+'/'+node_list, 'r')
    for node in f:
        if not node.startswith('#'):
            node_strs = node.split(",")
            region = node_strs[0].strip()
            nodes[region] = node_strs[1].strip()
    f.close()
    return nodes

# reads in all nodes and their corresponding codes 
# from the file located in the path <path_to_list>
# returns an ordered dictionary mapping each node's hostname to its code
def read_ec2_node_list_codes(path_to_list, node_list_codes):
    nodes = OrderedDict() # Want to map node hostnames to codes for abbreviated printing
    f = open(path_to_list+'/'+node_list_codes, 'r')
    for node in f:
        if not node.startswith('#'):
            node_strs = node.split(",")
            region = node_strs[0].strip()
            nodes[region] = node_strs[1].strip()
    f.close()
    return nodes
    
    
# connects to an instance
# returns the connection and the instance object
def connect_to_ec2_instance(region):
    # need to connect to the region
    conn = ec2.connect_to_region(region)
    
    # get the instance object: assume we only have one instance
    reservations = conn.get_all_reservations()
    instance = reservations[0].instances[0]

    return (conn, instance)
    
# starts an ec2 instance if it's stopped
def start_ec2_instance(conn, instance):
    updated_instance = instance
    if updated_instance.state == "stopped":
        conn.start_instances(instance_ids=[updated_instance.id])
        
        # wait for instance to come up
        while updated_instance.state != "running":
            updated_instance.update()
            status_obj = ec2.instancestatus.InstanceStatus(id=updated_instance.id)
        
        print("Instance is running")

        # ugh, it seems like EC2 doesn't create the details dict until after the
        # instance is running
        status = ""
        default = ""
        while status != "passed":
            status_obj = ec2.instancestatus.InstanceStatus(id=updated_instance.id)
            status = status_obj.instance_status.details.get("Status", default)
            print(status_obj.instance_status.details)
            time.sleep(10)
            
    return updated_instance

# stops an instance if it's running
def stop_ec2_instance(conn, instance):
    updated_instance = instance
    if updated_instance.state == "running":
        conn.stop_instances(instance_ids=[updated_instance.id])
        
        # wait for instance to stop
        while updated_instance.state != "stopped":
            updated_instance.update()
    
    return updated_instance

# runs the given command on the given ec2 instance
def run_cmd_on_ec2_instance(instance, user, cmd):
    call(['ssh', '-oStrictHostKeyChecking=no', user+'@'+instance.ip_address, cmd])

# puts the given file(s) on the from_path on the local machine 
# to the to_path on the given instance
def scp_to_ec2_instance(instance, user, from_path, to_path):
    for f in from_path:
        call(['scp', '-oStrictHostKeyChecking=no', f, user+'@'+instance.ip_address+':'+to_path])

# gets the given file(s) on the from_path on the given instance 
# to the to_path on the local machine
def scp_from_ec2_instance(instance, user, from_path, to_path):
    call(['scp', '-oStrictHostKeyChecking=no', user+'@'+instance.ip_address+':'+from_path, path+'/'+to_path])
