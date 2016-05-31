'''
Data collection tool for gathering website data from a list of EC2 nodes

author: Marcela S. Melara (melara@cs.princeton.edu)
date created: 10/21/2015
'''

# system imports
import sys
import time
from multiprocessing import Process

# external imports
from boto import ec2 # use boto 2, the API is better

import ec2_util

# runs the data collection on the given node
def collect_data_at_node(node, node_code, dataset_name, num_sites):
    # connect to the region
    (conn, instance) = ec2_util.connect_to_ec2_instance(node)

    # start the instance if it's stopped
    if instance.state != 'running':
         instance = ec2_util.start_ec2_instance(conn, instance)
         print('Started '+node_code)

    print('localhost: Starting crawler on '+node_code)

    # run the crawler command
    crawler_cmd = 'python3 crawler.py '+node_code+' '+dataset_name+' '+str(num_sites)+' /home/ubuntu'
    ec2_util.run_cmd_on_ec2_instance(instance, crawler_cmd)

    print('localhost: Data collection done at '+node_code)

    # before we retrieve the collected files, tar them (this is much faster than scp'ing the whole dataset)
    collected_data_tar = node_code+'_'+dataset_name+'.tar'
    tar_cmd = 'tar -cf '+collected_data_tar+' '+dataset_name+'/*'
    ec2_util.run_cmd_on_ec2_instance(instance, tar_cmd)

    print('localhost: Retrieving files from '+node_code)

    # now grab all the collected data
    # meh, gotta store it locally and then transfer to cycles later
    # otherwise we have to configure our AWS access keys and a separate ssh key 
    # for cycles, which is really annoying
    ec2_util.scp_from_ec2_instance(instance, path, '~/'+collected_data_tar, 'datasets')

    print('localhost: Done with '+node_code)

    # Stop the instance so we don't use up more resources
    instance = ec2_util.stop_ec2_instance(conn, instance)
    print('localhost: Stopped '+node_code)

## Main script ##
if len(sys.argv) != 2:
    print('Usage: python3 run_ec2_crawl.py <dataset name>')
    exit()

dataset_name = sys.argv[1]
path = ''
nodes = ec2_util.read_ec2_node_list(path)

n = 100

procs = []
for node, node_code in nodes.items():
    p = Process(target=collect_data_at_node, args=(node, node_code, dataset_name, n,))
    procs.append(p)
    p.start()
    print("localhost: "+node_code+" started ("+str(p.pid)+")")

for p in procs:
    p.join()
    print("localhost: "+node_code+" joined ("+str(p.pid)+")")


