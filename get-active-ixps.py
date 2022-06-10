#!/usr/bin/env python3

import json
import sys

if __name__ == '__main__':
    if len(sys.argv) > 1:
        ixp_data = str(sys.argv)[1]
    else:
        ixp_data = 'ixp-data.json'
    ixps = []
    with open(ixp_data) as json_file:
        ixpdata = json.load(json_file)
        for key, value in ixpdata.items():
            if value['active']:
                ixps.append(key)
    print(';'.join(ixps))
