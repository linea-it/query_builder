import json
from collections import OrderedDict
import re


def load_json(filename):
    with open(filename) as f:
        return json.load(f)
