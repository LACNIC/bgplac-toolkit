#!/usr/bin/env python3


import click
import csv
import json
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
@click.option('--source', default='data', help='directory where the data is stored')
def main(date, ixp, source):
    if date == '00000000':
        date = datetime.today().strftime('%Y%m%d')
        
    path = "{dir}/bgp-table-{ixp}-{date}.csv".format(dir=source, ixp=ixp, date=date)
    with open(path, newline='') as csvfile, open('../regions.json') as rirfile:
        regions = json.load(rirfile)
        cctorir = {}
        ixproutingtable = RoutingTable('lacnic', cctorir)

        for rir, countries in regions.items():
            for cc in countries:
                cctorir[cc] = rir

        reader = csv.DictReader(csvfile, delimiter=',', quoting=csv.QUOTE_NONE)
        for row in reader:
            pfx = row['prefix']
            asn_path = row['as_path'].split()
            cc_path = row['as_path_cc'].split()
            ixproutingtable.add_path(pfx, asn_path, cc_path)

        outp1 = "{dir}/ixp-routing-{ixp}-{date}.csv".format(dir=source, ixp=ixp, date=date)
        outp2 = "{dir}/aspath-freq-{ixp}-{date}.csv".format(dir=source, ixp=ixp, date=date)
        outp3 = "{dir}/preprend-freq-{ixp}-{date}.csv".format(dir=source, ixp=ixp, date=date)
        with open(outp1, 'w', newline='') as f1, open(outp2, 'w', newline='') as f2, open(outp3, 'w', newline='') as f3:
            w1 = csv.writer(f1)
            w1.writerow(["peer_cc", "peer_asn", "origin_cc", "origin_asn", "prefixes_ipv4", "prefixes_ipv6"])
            for pasn, peer in ixproutingtable.table.items():
                for oasn, origin in peer.branches.items():
                    w1.writerow([
                        peer.country,
                        pasn,
                        origin.country,
                        oasn,
                        " ".join(origin.prefixes4),
                        " ".join(origin.prefixes6)
                    ])
            w2 = csv.writer(f2)
            w2.writerow(["country", "hops", "frequency"])
            for cc, freq in ixproutingtable.hopstable.items():
                for h in sorted(freq):
                    w2.writerow([cc, h, freq[h]])
            w3 = csv.writer(f3)
            w3.writerow(["prepend", "frequency"])
            for p in sorted(ixproutingtable.prependstable):
                w3.writerow([p, ixproutingtable.prependstable[p]])


if __name__ == '__main__':
    main()
