#!/usr/bin/env python3


import click
import csv
import json
import sys
import pytricia
import pandas as pd
import urllib.request
from datetime import datetime


csv.field_size_limit(sys.maxsize)


class ResourceCatalog:

    def __init__(self):
        self.ptree = {
            'ipv4': pytricia.PyTricia(32),
            'ipv6': pytricia.PyTricia(128)
        }
        self.ases = {}

    def add_pfx(self, v, address, length, data):
        self.ptree[v].insert(address, length, data)

    def get_pfx(self, v, address):
        return self.ptree[v].get(address)

    def add_asn(self, asn, cc):
        self.ases[asn] = cc

    def get_asn(self, asn):
        if asn in self.ases:
            return self.ases[asn]
        else:
            return 'ZZ'


class RegionCatalog:

    def __init__(self):
        with open('regions.json') as json_file:
            self.regions = json.load(json_file)

    def get_region(self, cc):
        for rir, countries in self.regions.items():
            if cc in countries:
                return rir
        print(cc + ' region not found')
        return 'other'


class AsesDatabase:

    def __init__(self, catalog, rir):
        self.pfx_flow = {
            'afrinic': {},
            'apnic': {},
            'arin': {},
            'lacnic': {},
            'ripencc': {},
            'other': {}
        }
        self.as_flow = {
            'afrinic': {},
            'apnic': {},
            'arin': {},
            'lacnic': {},
            'ripencc': {},
            'other': {}
        }
        self.origin_ases = {}
        self.transit_ases = {}
        self.upstream_ases = {}
        for c in catalog.regions[rir]:
            self.origin_ases[c] = {}
            self.transit_ases[c] = {}

    def add_pfx_flow(self, orig, dest, pfx):
        if orig in self.pfx_flow[dest]:
            self.pfx_flow[dest][orig].add(pfx)
        else:
            self.pfx_flow[dest][orig] = set([pfx])
    
    def add_as_flow(self, orig, dest, asn):
        if orig in self.as_flow[dest]:
            self.as_flow[dest][orig].add(asn)
        else:
            self.as_flow[dest][orig] = set([asn])

    def add_transit_as(self, country, asn, ds_as):
        if asn in self.transit_ases[country]:
            self.transit_ases[country][asn].add(ds_as)
        else:
            self.transit_ases[country][asn] = set([ds_as])

    def add_upstream_as(self, country, asn, ds_as):
        if country in self.upstream_ases[asn]:
            self.upstream_ases[asn][country].add(ds_as)
        else:
            self.upstream_ases[asn][country] = set([ds_as])


def load_delegated(path):
    catalog = ResourceCatalog()
    print("* Retrieving delegated from " + path)
    with open(path, 'r', newline='') as content:
        print("* Procesing delegated")
        for line in content:
            row = line.split('|')
            if row[0] not in ['afrinic', 'apnic', 'arin', 'lacnic', 'ripencc']:
                continue
            if row[2] == 'asn':
                if row[1] != 'ZZ':
                    count = int(row[4])
                    start = int(row[3])
                    for i in range(count):
                        catalog.add_asn(str(start + i), row[1])
            elif row[2] == 'ipv4':
                catalog.add_pfx('ipv4', row[3], 33 - int(row[4]).bit_length(), row[1])
            elif row[2] == 'ipv6':
                catalog.add_pfx('ipv6', row[3], int(row[4]), row[1])
    print("DONE!")
    return catalog

def process_ases(path, res_catalog, reg_catalog, region):
    result = AsesDatabase(reg_catalog, region)
    countries = reg_catalog.regions[region]
    print("* Procesing ASes from " + path)
    with open(path, newline='') as f1:
        reader = csv.DictReader(f1, delimiter=',', quoting=csv.QUOTE_NONE)
        for row in reader:
            dst_asn = row['as']
            if dst_asn not in result.upstream_ases:
                result.upstream_ases[dst_asn] = {}
            dst_cc = row['cc']
            dst_rir = reg_catalog.get_region(dst_cc)
            for src_asn in row['downstream_ases'].split():
                src_cc = res_catalog.get_asn(src_asn)
                if src_cc in countries:
                    if src_cc == dst_cc:
                        result.add_transit_as(dst_cc, dst_asn, src_asn)
                    else:
                        result.add_as_flow(src_cc, dst_rir, src_asn)
                        result.add_upstream_as(src_cc, dst_asn, src_asn)
            for ipv4 in row['ipv4_downstream_prefixes'].split():
                src_cc = res_catalog.get_pfx('ipv4', ipv4)
                if src_cc in countries and src_cc != dst_cc:
                    result.add_pfx_flow(src_cc, dst_rir, ipv4)
            for ipv6 in row['ipv6_downstream_prefixes'].split():
                src_cc = res_catalog.get_pfx('ipv6', ipv6)
                if src_cc in countries and src_cc != dst_cc:
                    result.add_pfx_flow(src_cc, dst_rir, ipv6)
            if dst_cc in countries:
                pfxcount = len(row['ipv4_prefixes'].split()) + len(row['ipv6_prefixes'].split())
                if pfxcount > 0:
                    result.origin_ases[dst_cc][dst_asn] = pfxcount
    return result

def create_datasets(ts, source, result):
    with open(source + "/country-ases-flow-" + ts + ".csv", 'w', newline='') as f2:
        w2 = csv.writer(f2)
        w2.writerow(["origin", "destination", "ases"])
        for dst, srcs in result.as_flow.items():
            for src_cc, ases in srcs.items():
                w2.writerow([src_cc, dst, len(ases)])
    with open(source + "/country-prefixes-flow-" + ts + ".csv", 'w', newline='') as f3:
            w3 = csv.writer(f3)
            w3.writerow(["origin", "destination", "prefixes"])
            for dst, srcs in result.pfx_flow.items():
                for src_cc, pfxs in srcs.items():
                    w3.writerow([src_cc, dst, len(pfxs)])
    with open(source + "/country-origin-ases-" + ts + ".csv", 'w', newline='') as f4:
            w4 = csv.writer(f4)
            w4.writerow(["country", "asn", "prefixes"])
            for cc, ases in result.origin_ases.items():
                for asn, pfx_count in ases.items():
                    w4.writerow([cc, asn, pfx_count])
    with open(source + "/country-transit-ases-" + ts + ".csv", 'w', newline='') as f5:
            w5 = csv.writer(f5)
            w5.writerow(["country", "asn", "downstream_ases"])
            for cc, ases in result.transit_ases.items():
                for asn, ds_ases in ases.items():
                    w5.writerow([cc, asn, len(ds_ases)])
    with open(source + "/country-upstream-ases-" + ts + ".csv", 'w', newline='') as f6:
            w6 = csv.writer(f6)
            w6.writerow(["asn", "country", "downstream_ases"])
            for asn, countries in result.upstream_ases.items():
                for cc, ds_ases in countries.items():
                    w6.writerow([asn, cc, len(ds_ases)])
    print("- DONE!")

@click.command()
@click.option('--date', default='00000000', help='date of calculation')
@click.option('--source', default='data', help='directory where the data is stored')
@click.option('--region', default='lacnic', help='region to analize. see regions.json')
def main(date, source, region):
    if date == '00000000':
        date = datetime.today().strftime('%Y%m%d')
    regn_cat = RegionCatalog()
    numb_cat = load_delegated(source + '/delegated-' + date + '.csv')
    result = process_ases(source + '/as-data-' + date + '.csv', numb_cat, regn_cat, region)
    create_datasets(date, source, result)

if __name__ == '__main__':
    main()
