#!/usr/bin/env python
import os
from collections import defaultdict

hosts = {'mill028', 'mill029', 'mill030'}
user = 'a7109534'
file_location = '/work/a7109534/'
file_wildcard = '*'
extension = ".csv"
get_file = file_location + file_wildcard + extension
destination = '.'
number_of_rounds = 18

host_files = defaultdict(list)
for file in os.listdir(destination):
    for hostname in hosts:
        if hostname in file:
            host_files[hostname].append(file)
            host_files[hostname].sort()

x = 0
while True:
    host_files_iter = iter(host_files)
    next_host = host_files_iter.next()
        
    try:
        first_host = host_files.get(next_host)[x]
    except IndexError:
        break 
    
    for host in host_files_iter:
        second_host = host_files.get(host)[x]
        cmd = "diff " + first_host + " " + second_host + " -usa"
        os.system(cmd)
    x += 1
