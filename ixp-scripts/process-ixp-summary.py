#!/usr/bin/env python3


import sys
import click
import csv
import json
import ipaddress
import pandas as pd
import os
from datetime import datetime


@click.command()
@click.option('--date', default='00000000', help='date of calculation')
@click.option('--ixp', default='aep', help='ixp identifier')
@click.option('--src', default='data', help='source directory to retrieve data')
@click.option('--dst', default=False, help='directory where the data is stored')
@click.option('--subfolder/--no-subfolder', default=True, help='creates subfolder for ixp')
@click.option('--ixp-data', default='../ixp-data.json', help='directory where the ixp data is stored')
def main(date, ixp, src, dst, subfolder, ixp_data):
    if date == '00000000':
        date = datetime.today().strftime('%Y%m%d')
    
    if subfolder:
        src =  "{dir}/{ixp}".format(dir=src, ixp=ixp)
        if not dst:
            dst = src
        else:
            dst = "{dir}/{ixp}".format(dir=dst, ixp=ixp)
            os.makedirs(os.path.dirname(dst), exist_ok=True)
    elif not dst:
        dst = src

    inpath1 = "{dir}/ixp-routing-{ixp}-{date}.csv".format(dir=src, ixp=ixp, date=date)
    inpath2 = "{dir}/aspath-freq-{ixp}-{date}.csv".format(dir=src, ixp=ixp, date=date)
    outpath = "{dir}/ixp-summary-{ixp}-{date}.csv".format(dir=dst, ixp=ixp, date=date)

    if ixp_data.startswith('/'):
        ixpdata_path = ixp_data
    else:
        ixpdata_path = os.path.join(sys.path[0], ixp_data)
    with open(os.path.join(sys.path[0], ixp_data)) as ixpfile, open(ixpdata_path) as rirfile, open(inpath1, newline='') as infile:
        ixpdata = json.load(ixpfile)
        if ixp not in ixpdata:
            raise Exception("IXP not found")
        selected = ixpdata[ixp]

        regions = json.load(rirfile)
        cctorir = {}
        for rir, countries in regions.items():
            for cc in countries:
                cctorir[cc] = rir

        table = {}
        rdr = csv.DictReader(infile)
        for row in rdr:
            peercc = row["peer_cc"]
            origcc = row["origin_cc"]
            if peercc in cctorir:
                if cctorir[peercc] != selected['region']:
                    peercc = cctorir[peercc]
            else:
                peercc = 'other'
            if peercc not in table:
                table[peercc] = { 'peer_ases': set([]), 'origin_ases': set([]), 'origin_prefixes_ipv4': set([]), 'origin_prefixes_ipv6': set([]) }
            if origcc not in table:
                table[origcc] = { 'peer_ases': set([]), 'origin_ases': set([]), 'origin_prefixes_ipv4': set([]), 'origin_prefixes_ipv6': set([]) }
            table[peercc]['peer_ases'].add(row['peer_asn'])
            custom = table[origcc]
            custom['origin_ases'].add(row['origin_asn'])
            custom['origin_prefixes_ipv4'].update(row['prefixes_ipv4'].split())
            custom['origin_prefixes_ipv6'].update(row['prefixes_ipv6'].split())
        
        dataset = []
        for cc, row in table.items():
            prefixes4 = []
            pfxlen4_sum = 0
            prefixes6 = []
            pfxlen6_sum = 0
            for p in row['origin_prefixes_ipv4']:
                ipnet = ipaddress.IPv4Network(p)
                pfxlen4_sum += ipnet.prefixlen
                prefixes4.append(ipnet)
            for p in row['origin_prefixes_ipv6']:
                ipnet = ipaddress.IPv6Network(p)
                pfxlen6_sum += ipnet.prefixlen
                prefixes6.append(ipnet)
            origin_collapsed_prefixes4 = ipaddress.collapse_addresses(prefixes4)
            origin_collapsed_prefixes6 = ipaddress.collapse_addresses(prefixes6)
            addresses4_count = 0
            for net in origin_collapsed_prefixes4:
                addresses4_count += net.num_addresses
            addresses6_count = 0
            for net in origin_collapsed_prefixes6:
                addresses6_count += net.num_addresses
            prefixes4_count = len(row['origin_prefixes_ipv4'])
            if prefixes4_count > 0:
                origin_pfxlen4_avg = "{:.2f}".format(pfxlen4_sum/prefixes4_count)
            else:
                origin_pfxlen4_avg = ''
            prefixes6_count = len(row['origin_prefixes_ipv6'])
            if prefixes6_count > 0:
                origin_pfxlen6_avg = "{:.2f}".format(pfxlen6_sum/prefixes6_count)
            else:
                origin_pfxlen6_avg = ''
            dataset.append({
                "country": cc,
                "peer_ases_count": len(row['peer_ases']),
                "origin_ases_count": len(row['origin_ases']),
                "origin_prefixes_ipv4_count": prefixes4_count,
                "origin_prefixes_ipv6_count": prefixes6_count,
                "origin_prefixes_count": prefixes4_count + prefixes6_count,
                "origin_addresses_ipv4_count": addresses4_count,
                "origin_addresses_ipv6_count": addresses6_count,
                "origin_pfxlen_ipv4_avg": origin_pfxlen4_avg,
                "origin_pfxlen_ipv6_avg": origin_pfxlen6_avg
            })
        df = pd.DataFrame(dataset)

        data = pd.read_csv(inpath2)
        g = data.groupby('country')
        data['wa'] = data.frequency / g.frequency.transform("sum") * data.hops
        aggregated = g.agg({
            'wa': 'sum',
            'hops': ['min', 'max'],
            'frequency': 'sum'
        })
        aggregated.columns = ["_".join(x) for x in aggregated.columns.ravel()]
        aggregated.rename(columns={
            'wa_sum': 'aspath_len_avg',
            'hops_min': 'aspath_len_min',
            'hops_max': 'aspath_len_max',
            'frequency_sum': 'total_paths'
        }, inplace=True)
        outer_merged = pd.merge(df, aggregated, how="outer", on='country')
        outer_merged.to_csv(outpath, index=False, float_format='%.2f')


if __name__ == '__main__':
    main()
