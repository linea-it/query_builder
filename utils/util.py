import json
from collections import OrderedDict
import re


def load_json(filename):
    with open(filename) as f:
        return json.load(f)


def dot_file_to_dict(file_name):
    COMMENT = "^\s*#"
    NEW_LINE = "^\s*$"
    ARC = "^\s*(\S+)\s*->\s*(\S+)\s*$"

    tree = {}
    with open(file_name) as f:
        lines = f.read().splitlines()

    for line in lines:
        p = re.compile(COMMENT)
        if p.match(line):
            continue
        p = re.compile(NEW_LINE)
        if p.match(line):
            continue
        p = re.compile(ARC)
        if p.match(line):
            groups = p.match(line).groups()
            if groups[0] not in tree:
                tree[groups[0]] = []
            tree[groups[0]].append(groups[1])
            continue
        raise "invalid format"
    return tree
