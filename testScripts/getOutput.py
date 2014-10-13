#!/usr/bin/env python
import os
from collections import defaultdict

hosts = {'mill001', 'mill002', 'mill004', 'mill006', 'mill007', 'mill008'}
user = 'a7109534'
file_location = '/work/a7109534/'
#file_location = '/home/ryan/workspace/JGroups'
#file_location = '/home/pg/p11/a7109534/'
file_wildcard = 'mill*'
extension = "*"
get_file = file_location + file_wildcard + extension
destination = '.'
number_of_rounds = 18

for hostname in hosts:
    cmd = "scp " + user + "@" + hostname + ":" + get_file + " " + destination
    print cmd
    os.system(cmd)
