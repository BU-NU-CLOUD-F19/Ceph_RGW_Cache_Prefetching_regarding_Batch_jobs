#!/usr/bin/python
import json

with open('./simulation_result.json') as json_file:
    jdata = json.load(json_file)
    print(json.dumps(jdata, indent = 4))
