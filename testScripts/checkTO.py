#!/usr/bin/env python
import os
from collections import defaultdict

hosts = {'mill002', 'mill004'}
user = 'a7109534'
file_location = '/work/a7109534/'
#file_location = '/home/ryan/workspace/JGroups'
#file_location = '/home/pg/p11/a7109534/'
file_wildcard = '*'
extension = ".csv"
get_file = file_location + file_wildcard + extension
destination = '.'
number_of_rounds = 400

os.system("rm *" + extension)
for hostname in hosts:
    cmd = "scp " + user + "@" + hostname + ":" + get_file + " " + destination
    print cmd
    os.system(cmd)

host_files = defaultdict(list)
for file in os.listdir(destination):
    for hostname in hosts:
        if hostname in file:
            host_files[hostname].append(file)
            host_files[hostname].sort()

for x in xrange(number_of_rounds):
    host_files_iter = iter(host_files)
    first_host = host_files.get(host_files_iter.next())[x]
    for host in host_files_iter:
        second_host = host_files.get(host)[x]
        cmd = "diff " + first_host + " " + second_host + " -usa"
        os.system(cmd)
