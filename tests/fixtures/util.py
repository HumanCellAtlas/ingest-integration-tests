import json
from os import listdir
from os.path import isfile, join


def load_file(location):
    with open(location, 'r') as f:
        obj = json.load(f)
    return obj


def load_files(dir):
    obj_list = []
    files = [f for f in listdir(dir) if isfile(join(dir, f))]
    for file in files:
        obj_list.append(load_file(join(dir, file)))
    return obj_list
