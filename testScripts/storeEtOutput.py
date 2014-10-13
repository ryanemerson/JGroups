#!/usr/bin/env python
import os
import argparse
import shutil

OUTPUT_DIR = '/home/ryan/Dropbox/PhD/Results/CloudCom/Emulated Transactions/TOA-BOX/4th Run/3 Nodes/'
FOLDER_NAME = 'AC'
FILE_PATTERN = 'mill0'

def is_valid_path(arg):
    if os.path.isdir(arg):
        return os.path.abspath(arg)
    else:
        raise argparse.ArgumentTypeError('Specified path does not exist or is not a directory')

parser = argparse.ArgumentParser(description='Process experiment output and move files to specified dir')
parser.add_argument('integer', type=int,
                    metavar='N',
                    help='The number of anycast addresses used in the experiment')
args = parser.parse_args()

newPath = OUTPUT_DIR + FOLDER_NAME + str(args.integer) + '/'
if not os.path.exists(newPath):
    os.makedirs(newPath)
    files = [f for f in os.listdir('.') if os.path.isfile(f) and (f.startswith(FILE_PATTERN) or f == 'results.out')]
    for f in files:
        shutil.move(f,newPath)
else:
    print 'Dir : ' + newPath + ' already exists'
