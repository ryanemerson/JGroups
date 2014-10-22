#!/usr/bin/env python
import os
import argparse
import shutil

OUTPUT_DIR = '/home/a7109534/Dropbox/PhD/Results/DSN/Box Simulation/Aramis and Base/'
FOLDER_NAME = 'Experiment'
EXT_TO_MOVE = 'csv'

def is_valid_path(arg):
    if os.path.isdir(arg):
        return os.path.abspath(arg)
    else:
        raise argparse.ArgumentTypeError('Specified path does not exist or is not a directory')

parser = argparse.ArgumentParser(description='Process experiment output and move files to specified dir')
parser.add_argument('integer', type=int,
                    metavar='N',
                    help='The experiment number associated with the files to be moved')
args = parser.parse_args()

newPath = OUTPUT_DIR + FOLDER_NAME + str(args.integer) + '/'
if not os.path.exists(newPath):
    os.makedirs(newPath)
    files = [f for f in os.listdir('.') if os.path.isfile(f) and f.endswith(EXT_TO_MOVE) or f.endswith('.txt')]
    for f in files:
        shutil.move(f,newPath)
else:
    print 'Dir : ' + newPath + ' already exists'
