#!/usr/bin/env python3


import sys
import click
import csv
import json
import os
from datetime import datetime


class RoutingCountry:

    def __init__(self, cc):
        self.country = cc
        self.branches = {}
        self.prefixes4 = set([])
        self.prefixes6 = set([])

    def add_prefix(self, prefix):
        if '.' in prefix:
            self.prefixes4.add(prefix)
        else:
            self.prefixes6.add(prefix)


class RoutingTable:

    def __init__(self, region, countries):
        self.table = {}
        self.hopstable = {}
        self.prependstable = {}
        self.region = region
        self.countries = countries
        self.prepends = 0

    def add_path(self, prefix, asn_path, cc_path):
        peer = asn_path[0]
        peer_cc = cc_path[0]
        origin = asn_path[-1]
        origin_cc = cc_path[-1]
        if origin_cc in self.countries:
            if self.countries[origin_cc] != self.region:
                origin_cc = self.countries[origin_cc]
        else:
            origin_cc = 'other'
        if peer not in self.table:
            self.table[peer] = RoutingCountry(peer_cc)
        pcountry = self.table[peer]
        if origin not in pcountry.branches:
            pcountry.branches[origin] = RoutingCountry(origin_cc)
        pcountry.branches[origin].add_prefix(prefix)
        # loading as path hops table
        if origin_cc not in self.hopstable:
            self.hopstable[origin_cc] = {}
        hops = len(asn_path)
        if hops not in self.hopstable[origin_cc]:
            self.hopstable[origin_cc][hops] = 0
        self.hopstable[origin_cc][hops] += 1
        # counting prepends
        if hops > 1:
            nopreps = [a for a in asn_path if a != origin]
            prepend_length = hops - len(nopreps)
            if prepend_length > 1:
                if prepend_length not in self.prependstable:
                    self.prependstable[prepend_length] = 0
                self.prependstable[prepend_length] += 1


@click.command()
@click.option('--date', default='00000000', help='date of calculation')
@click.option('--ixp', default='aep', help='ixp identifier')
@click.option('--src', default='data', help='source directory to retrieve data')
@click.option('--dst', default=False, help='directory where the data is stored')
@click.option('--global-src', default=False, help='directory where the global tables data is stored')
@click.option('--subfolder/--no-subfolder', default=True, help='creates subfolder for ixp')
@click.option('--ixp-data', default='../ixp-data.json', help='directory where the ixp data is stored')
def main(date, ixp, src, dst, global_src, subfolder, ixp_data):
    if date == '00000000':
        date = datetime.today().strftime('%Y%m%d')
    if not global_src:
        global_src = src

    
    if subfolder:
        src =  "{dir}/{ixp}".format(dir=src, ixp=ixp)
        if not dst:
            dst = src
        else:
            dst = "{dir}/{ixp}".format(dir=dst, ixp=ixp)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
    elif not dst:
        dst = src
        
    fix = "{dir}/ixp-routing-{ixp}-{date}.csv".format(dir=src, ixp=ixp, date=date)
    fcc = "{dir}/prefix-data-{date}.csv".format(dir=global_src, date=date)
    if ixp_data.startswith('/'):
        ixpdata_path = ixp_data
    else:
        ixpdata_path = os.path.join(sys.path[0], ixp_data)
    with open(fix, newline='') as csvix, open(fcc, newline='') as csvcc, open(ixpdata_path) as json_file:

        ixpdata = json.load(json_file)
        if ixp not in ixpdata:
            raise Exception("IXP not found")
        country = ixpdata[ixp]['country']

        ix_pf4s = set([])
        ix_pf6s = set([])
        ix_asns = set([])

        cc_pf4s = set([])
        cc_pf6s = set([])
        cc_asns = set([])

        rix = csv.DictReader(csvix, delimiter=',', quoting=csv.QUOTE_NONE)
        for row in rix:
            if row['origin_cc'] == country:
                ix_asns.add(row['origin_asn'])
                ix_pf4s.update(row['prefixes_ipv4'].split())
                ix_pf6s.update(row['prefixes_ipv6'].split())
        
        rcc = csv.DictReader(csvcc, delimiter=',', quoting=csv.QUOTE_NONE)
        for row in rcc:
            if row['country'] == country:
                cc_asns.add(row['origin_asn'])
                prefix = "{0}/{1}".format(row['prefix'], row['length'])
                if row['version'] == 'ipv4':
                    cc_pf4s.add(prefix)
                else:
                    cc_pf6s.add(prefix)

        shared_asns = ix_asns.intersection(cc_asns)
        ixonly_asns = ix_asns.difference(shared_asns)
        cconly_asns = cc_asns.difference(shared_asns)

        shared_pf6s = ix_pf6s.intersection(cc_pf6s)
        ixonly_pf6s = ix_pf6s.difference(shared_pf6s)
        cconly_pf6s = cc_pf6s.difference(shared_pf6s)

        shared_pf4s = ix_pf4s.intersection(cc_pf4s)
        ixonly_pf4s = ix_pf4s.difference(shared_pf4s)
        cconly_pf4s = cc_pf4s.difference(shared_pf4s)

        outp1 = "{dir}/country-coverage-{ixp}-{date}.csv".format(dir=dst, ixp=ixp, date=date)
        outp2 = "{dir}/country-coverage-summary-{ixp}-{date}.csv".format(dir=dst, ixp=ixp, date=date)
        with open(outp1, 'w', newline='') as f1, open(outp2, 'w', newline='') as f2:
            w1 = csv.writer(f1)
            w1.writerow(["resource", "shared", "ixp_only", "country_only"])
            w1.writerow([
                'asn', " ".join(shared_asns), " ".join(ixonly_asns), " ".join(cconly_asns)
            ])
            w1.writerow([
                'ipv4', " ".join(shared_pf4s), " ".join(ixonly_pf4s), " ".join(cconly_pf4s)
            ])
            w1.writerow([
                'ipv6', " ".join(shared_pf6s), " ".join(ixonly_pf6s), " ".join(cconly_pf6s)
            ])
            w2 = csv.writer(f2)
            w2.writerow(["resource", "total", "shared", "ixp_only", "country_only"])
            w2.writerow([
                'asn', len(shared_asns)+len(ixonly_asns)+len(cconly_asns), len(shared_asns), len(ixonly_asns), len(cconly_asns)
            ])
            w2.writerow([
                'ipv4', len(shared_pf4s)+len(ixonly_pf4s)+len(cconly_pf4s), len(shared_pf4s), len(ixonly_pf4s), len(cconly_pf4s)
            ])
            w2.writerow([
                'ipv6', len(shared_pf6s)+len(ixonly_pf6s)+len(cconly_pf6s), len(shared_pf6s), len(ixonly_pf6s), len(cconly_pf6s)
            ])


if __name__ == '__main__':
    main()
