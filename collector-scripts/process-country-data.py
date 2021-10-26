#!/usr/bin/env python3


import click
import csv
from datetime import datetime


@click.command()
@click.option('--date', default='00000000', help='date of calculation')
@click.option('--source', default='data', help='directory where the data is stored')
def main(date, source):
    if date == '00000000':
        date = datetime.today().strftime('%Y%m%d')
    inpath = source + "/country-data-" + date + ".csv"
    outpath = source + "/country-summary-" + date + ".csv"
    print("* Procesing countries from " + inpath)
    with open(inpath, newline='') as infile, open(outpath, 'w', newline='') as outfile:
        rdr = csv.DictReader(infile)
        wrt = csv.writer(outfile)
        wrt.writerow([
            "country",
            "total_origin_asns", "total_transit_asns", "total_upstream_asns", "total_unregistered_asns", "total_offshore_asns",
            "ipv4_origin_asns", "ipv4_transit_asns", "ipv4_upstream_asns", "ipv4_unregistered_asns", "ipv4_offshore_asns",
            "ipv6_origin_asns", "ipv6_transit_asns", "ipv6_upstream_asns", "ipv6_unregistered_asns", "ipv6_offshore_asns",
            "total_local_asns"
        ])
        for row in rdr:
            ipv4_orgs = row["ipv4_origin_asns"].split()
            ipv4_trns = row["ipv4_transit_asns"].split()
            ipv4_upss = row["ipv4_upstream_asns"].split()
            ipv4_unrs = row["ipv4_unregistered_asns"].split()
            ipv4_offs = row["ipv4_offshore_asns"].split()
            ipv6_orgs = row["ipv6_origin_asns"].split()
            ipv6_trns = row["ipv6_transit_asns"].split()
            ipv6_upss = row["ipv6_upstream_asns"].split()
            ipv6_unrs = row["ipv6_unregistered_asns"].split()
            ipv6_offs = row["ipv6_offshore_asns"].split()
            total_orgs = set().union(ipv4_orgs, ipv6_orgs)
            total_trns = set().union(ipv4_trns, ipv6_trns)
            wrt.writerow([
                row["country"],
                len(total_orgs),
                len(total_trns),
                len(set().union(ipv4_upss, ipv6_upss)),
                len(set().union(ipv4_unrs, ipv6_unrs)),
                len(set().union(ipv4_offs, ipv6_offs)),
                len(ipv4_orgs),
                len(ipv4_trns),
                len(ipv4_upss),
                len(ipv4_unrs),
                len(ipv4_offs),
                len(ipv6_orgs),
                len(ipv6_trns),
                len(ipv6_upss),
                len(ipv6_unrs),
                len(ipv6_offs),
                len(total_orgs.union(total_trns))
            ])
        print("- DONE!")


if __name__ == '__main__':
    main()
