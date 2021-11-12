import json


if __name__ == '__main__':
    ixps = []
    with open('ixp-data.json') as json_file:
        ixpdata = json.load(json_file)
        for key, value in ixpdata.items():
            if value['active']:
                ixps.append(key)
    print(';'.join(ixps))
