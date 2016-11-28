import os
import json
RELATIVE_NAME = 'inout/data.json'


DATA = {}
with open(RELATIVE_NAME) as data_file:
    DATA = json.load(data_file)


OPERATIONS = DATA['input']['operations']
PROCESS = DATA['input']['process']
