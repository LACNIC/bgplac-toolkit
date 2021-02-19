#!/usr/bin/env python3


import click
import pytricia
import pybgpstream
import csv
import json
import urllib.request
from datetime import datetime


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


class RoutingDatabase:

    def __init__(self, countries_list, catalog):
        self.anomalies = []
        self.ases = {}
        self.countries = {}
        self.resources = catalog
        for c in countries_list:
            self.countries[c] = {
                'ipv4_origin_asns': set([]),
                'ipv4_transit_asns': set([]),
                'ipv4_upstream_asns': set([]),
                'ipv4_unregistered_asns': set([]),
                'ipv4_offshore_asns': set([]),
                'ipv6_origin_asns': set([]),
                'ipv6_transit_asns': set([]),
                'ipv6_upstream_asns': set([]),
                'ipv6_unregistered_asns': set([]),
                'ipv6_offshore_asns': set([])
            }
        self.pfxs = {}

    def add_anomaly(self, pfx, path):
        self.anomalies.append(pfx + '|' + ' '.join(path))

    def add_prefix_to_as(self, asn, cc, pfx, origin, v, prevs):
        if asn not in self.ases:
            self.ases[asn] = {
                'country': cc,
                'ipv4_prefixes': set([]),
                'ipv6_prefixes': set([]),
                'ipv4_downstream_prefixes': set([]),
                'ipv6_downstream_prefixes': set([]),
                'downstream_ases': set([])
            }
        self.ases[asn]['downstream_ases'] |= prevs
        if origin:
            self.ases[asn][v + '_prefixes'].add(pfx)
        else:
            self.ases[asn][v + '_downstream_prefixes'].add(pfx)

    def add_path(self, prefix, prefix_cc, path, v):
        origin = path[-1]
        if not origin.isdigit():
            origin = origin[1:-1]
            path[-1] = origin
            if not origin.isdigit():
                self.add_anomaly(prefix, path)
                return
        origin_cc = self.resources.get_asn(origin)
        self.add_prefix_to_as(origin, origin_cc, prefix, True, v, set([]))
        if origin_cc == prefix_cc:
            self.countries[prefix_cc][v + '_origin_asns'].add(origin)
            prevs = set([origin])
            for asn in path[-2:0:-1]:
                if not asn.isdigit():
                    self.add_anomaly(prefix, path)
                if asn not in prevs:
                    asn_cc = self.resources.get_asn(asn)
                    self.add_prefix_to_as(asn, asn_cc, prefix, False, v, prevs)
                    if asn_cc == prefix_cc:
                        self.countries[prefix_cc][v + '_transit_asns'].add(asn)
                    else:
                        self.countries[prefix_cc][v + '_upstream_asns'].add(asn)
                        break
                    prevs.add(asn)
        else:
            if origin_cc == 'ZZ':
                self.countries[prefix_cc][v + '_unregistered_asns'].add(origin)
            else:
                self.countries[prefix_cc][v + '_offshore_asns'].add(origin)
        # pfx db
        if prefix in self.pfxs:
            self.pfxs[prefix]['jumps'] += len(path)
            self.pfxs[prefix]['paths'] += 1
        else:
            add_len = prefix.split('/')
            self.pfxs[prefix] = {
                'prefix': add_len[0],
                'length': add_len[1],
                'version': v,
                'cc': prefix_cc,
                'origin': path[-1],
                'jumps': len(path),
                'paths': 1
            }


def load_countries(region):
    with open('regions.json') as json_file:
        data = json.load(json_file)
        if region in data:
            return data[region]
    return []

def load_delegated(path):
    catalog = ResourceCatalog()
    print("* Retrieving delegated from " + path)
    with open(path, 'r', newline='') as content:
        print("* Processing delegated")
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
    return catalog

def process_ribs(ts, collectors, countries, catalog):
    date = ts[0:4] + '-' + ts[4:6] + '-' + ts[6:8]
    print("* Processing RIBs from " + ts + " (" + ", ".join(collectors) + ")")
    computed_lines = 0
    stream = pybgpstream.BGPStream(
        from_time=date+" 07:50:00", until_time=date+" 08:10:00",
        collectors=collectors,
        record_type="ribs",
    )
    result = RoutingDatabase(countries, catalog)
    for rec in stream.records():
        for elem in rec:
            prefix = elem.fields["prefix"]
            path = elem.fields["as-path"].split()
            if '.' in prefix:
                v = 'ipv4'
            else:
                v = 'ipv6'
            prefix_cc = catalog.get_pfx(v, prefix)
            if prefix_cc in countries:
                result.add_path(prefix, prefix_cc, path, v)
            computed_lines += 1
            if computed_lines % 1000 == 0:
                print('\r* ' + str(computed_lines) + " rows computed", end="", flush=True)
    print('\r* ' + str(computed_lines) + " total rows computed\n", end="", flush=True)
    return result

def create_datasets(ts, source, result):
    print("* Creating datasets")
    with open(source + "/country-data-" + ts + ".csv", 'w', newline='') as f1:
        w1 = csv.writer(f1)
        w1.writerow([
            "country",
            "ipv4_origin_asns", "ipv4_transit_asns", "ipv4_upstream_asns", "ipv4_unregistered_asns", "ipv4_offshore_asns",
            "ipv6_origin_asns", "ipv6_transit_asns", "ipv6_upstream_asns", "ipv6_unregistered_asns", "ipv6_offshore_asns"
        ])
        for cc, values in result.countries.items():
            w1.writerow([
                cc,
                " ".join(values["ipv4_origin_asns"]),
                " ".join(values["ipv4_transit_asns"]),
                " ".join(values["ipv4_upstream_asns"]),
                " ".join(values["ipv4_unregistered_asns"]),
                " ".join(values["ipv4_offshore_asns"]),
                " ".join(values["ipv6_origin_asns"]),
                " ".join(values["ipv6_transit_asns"]),
                " ".join(values["ipv6_upstream_asns"]),
                " ".join(values["ipv6_unregistered_asns"]),
                " ".join(values["ipv6_offshore_asns"])
            ])
    with open(source + "/prefix-data-" + ts + ".csv", 'w', newline='') as f2:
        w2 = csv.writer(f2)
        w2.writerow(["prefix", "length", "version", "country", "origin_asn", "jumps", "paths"])
        for i, values in result.pfxs.items():
            w2.writerow([
                values["prefix"],
                values["length"],
                values["version"],
                values["cc"],
                values["origin"],
                values["jumps"],
                values["paths"],
            ])
    with open(source + "/as-data-" + ts + ".csv", 'w', newline='') as f3:
        w3 = csv.writer(f3)
        w3.writerow([
            "as", "cc", "downstream_ases",
            "ipv4_prefixes", "ipv4_downstream_prefixes",
            "ipv6_prefixes", "ipv6_downstream_prefixes"
        ])
        for a, data in result.ases.items():
            w3.writerow([
                a,
                data["country"],
                " ".join(data["downstream_ases"]),
                " ".join(data["ipv4_prefixes"]),
                " ".join(data["ipv4_downstream_prefixes"]),
                " ".join(data["ipv6_prefixes"]),
                " ".join(data["ipv6_downstream_prefixes"])
            ])
    print("- DONE!")

@click.command()
@click.option('--date', default='00000000', help='date of calculation')
@click.option('--collectors', default='rrc00', help='bgp collectors to use')
@click.option('--source', default='data', help='directory where the data is stored')
@click.option('--region', default='lacnic', help='region to analize. see regions.json')
def main(date, collectors, source, region):
    if date == '00000000':
        date = datetime.today().strftime('%Y%m%d')
    countries = load_countries(region)
    catalog = load_delegated(source + '/delegated-' + date + '.csv')
    result = process_ribs(date, collectors.split(','), countries, catalog)
    create_datasets(date, source, result)
    print("! " + str(len(result.anomalies)) + " anomalies found")
    # for a in result.anomalies:
    #     print(a)


if __name__ == '__main__':
    main()
