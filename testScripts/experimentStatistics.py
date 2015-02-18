#!/usr/bin/env python
import os
import argparse
import subprocess

def shellquote(s):
        return "'" + s.replace("'", "'\\''") + "'"

def is_valid_path(arg):
    if os.path.isdir(arg):
        return shellquote(os.path.abspath(arg))
    else:
        raise argparse.ArgumentTypeError('Specified path does not exist or is not a directory')

parser = argparse.ArgumentParser(description='Script to provide statistics from Emulated Transaction experiments')
parser.add_argument('-d', '--directory', help='directory to use', default='.', action='store', type=is_valid_path)
args = parser.parse_args()

print "Base Directory:= " + args.directory + "\n"

keywords = ['MESSAGES_TIMEDOUT', 'TOTAL_ORDER_MSGS', 'REQUESTS_RECEIVED']
output = list()

for key in keywords:
    grep_string = "grep --include=*.out -Proh '(?<=" + key + "=)[1-9][0-9]*' " + args.directory
    awk_string = "| awk 'BEGIN { MAX = 0; MIN = 0 }" \
                "{ SUM += $1; COUNT++; if ($1 > MAX) MAX = $1; if ($1 < MIN) MIN = $1}" \
                "END { MEAN = 0; if (COUNT + 0 != 0) MEAN = SUM / COUNT; print \"%s | COUNT := \"COUNT\" | SUM := \"SUM\" | MEAN := \"MEAN\" | RANGE := \"MAX - MIN}'" % (key)
    output.append(subprocess.check_output(grep_string + awk_string, shell=True))

print '\n'.join(output)
