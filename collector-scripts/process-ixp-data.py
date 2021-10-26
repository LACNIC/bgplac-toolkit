#!/usr/bin/env python3


import sys
import click
import json
import csv
import os
import urllib.request
from datetime import datetime
import ssl


def load_countries(region):
    with open(os.path.join(sys.path[0], '../regions.json')) as json_file:
        data = json.load(json_file)
        if region in data:
            return data[region]
    return []


@click.command()
@click.option('--date', default='00000000', help='date of calculation')
@click.option('--source', default='data', help='directory where the data is stored')
@click.option('--region', default='lacnic', help='region to analize. see regions.json')
def main(date, source, region):
    if date == '00000000':
        date = datetime.today().strftime('%Y%m%d')
    outpath = source + "/ixp-summary-" + date + ".csv"
    countries = load_countries(region)
    db = {}
    for c in countries:
        db[c] = {
            'ixp_count': 0
        }
    with open(outpath, 'w', newline='') as outfile:
        year = int(date[0:4])
        month = date[4:6]
        if year < 2019:
            dsdate = '201810'
        elif month in ['02', '03', '04']:
            dsdate = str(year) + '01'
        elif month in ['05', '06', '07']:
            dsdate = str(year) + '04'
        elif month in ['08', '08', '10']:
            dsdate = str(year) + '07'
        elif month in ['11', '12']:
            dsdate = str(year) + '10'
        else:
            dsdate = str(year-1) + '10'
        print("* Retrieving IXP data from " + dsdate)
        url = "https://data.caida.org/datasets/ixps/ixps_v2/ixs_" + dsdate + ".jsonl"
        content = urllib.request.urlopen(url)
        print("* Processing IXP dataset")
        for l in content:
            line = l.decode('utf-8')
            if line[0] != '#':
                obj = json.loads(line)
                if 'country' in obj:
                    cc = obj['country']
                    sources = obj['sources']
                    if cc in countries and 'pch' in sources:
                        db[cc]['ixp_count'] += 1
                        # print(cc, obj['name'], obj['ix_id'], obj['sources'])
        writer = csv.writer(outfile)
        writer.writerow(["country", "ixp_count"])
        for prefix, values in db.items():
            writer.writerow([
                prefix,
                values["ixp_count"]
            ])
        print("- DONE!")


if __name__ == '__main__':
    main()
