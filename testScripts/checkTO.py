#!/usr/bin/env python
import os
from collections import defaultdict

hosts = {'mill026', 'mill027', 'mill030'}
user = 'a7109534'
file_location = '/work/a7109534/'
file_wildcard = '*'
get_file = file_location + file_wildcard
destination = '.'
number_of_rounds = 4
extension = ".csv"

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