#!/usr/bin/env python
import os
from collections import defaultdict

hosts = {'mill028', 'mill029', 'mill030'}
user = 'a7109534'
file_location = '/work/a7109534/'
extension = "*"
get_file = file_location + extension
destination = '.'
number_of_rounds = 1000

for hostname in hosts:
    cmd = "scp " + user + "@" + hostname + ":" + get_file + " " + destination
    print cmd
    os.system(cmd)
