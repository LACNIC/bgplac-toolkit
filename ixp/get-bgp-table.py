#!/usr/bin/env python3


import click
import csv
import gzip
import json
import re
import pytricia
import pybgpstream
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
        if address in self.ptree[v]:
            return self.ptree[v].get(address)
        print("! WARNING! {v} {net} country not found".format(v=v, net=address))
        return 'ZZ'

    def add_asn(self, asn, cc):
        self.ases[asn] = cc

    def get_asn(self, asn):
        if asn in self.ases:
            return self.ases[asn]
        else:
            return 'ZZ'
    
    def load_delegated(self, path):
        print("* Retrieving delegated from {path}".format(path=path))
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
                            self.add_asn(str(start + i), row[1])
                elif row[2] == 'ipv4':
                    self.add_pfx('ipv4', row[3], 33 - int(row[4]).bit_length(), row[1])
                elif row[2] == 'ipv6':
                    self.add_pfx('ipv6', row[3], int(row[4]), row[1])

def addprefix(ip):
    spl = ip.split('/')
    if len(spl) == 1:
        spl = ip.split('.')
        if len(spl) > 1:
            a = int(spl[0])
            if a < 128:
                return ip + '/' + '8'
            elif a < 192:
                return ip + '/' + '16'
            elif a < 224:
                return ip + '/' + '24'
    return ip

def getpath(strpath):
    path = strpath.split()
    if not path:
        return False
    origin = path[-1]
    if not origin.isdigit():
        origin = origin[1:-1]
        path[-1] = origin
        if not origin.isdigit():
            return False
    return path

def process_pch(url, ipv, catalog, writer):
    urllib.request.urlretrieve(url, "{dir}/temp.gz".format(dir='.'))
    with gzip.open("{dir}/temp.gz".format(dir='.'),'rt') as fgzp:
        is_header = True
        ipv46 = '[A-Za-z0-9:\.]+'
        netre = '^...(' + ipv46 + ')\/?(\d+)?\s*(.*)$'
        prefix = ''
        for line in fgzp:
            if is_header:
	            if 'Network' in line:
		            is_header = False
            else:
                if line[0:8] == 'Displayed':
                    print('EOF')
                    break
                elif len(line) > 61:
                    spath = line[61:-3].strip()
                    path = getpath(spath)
                    match = re.match(netre, line)
                    if match:
                        long = match.group(2)
                        if long is None:
                            prefix = addprefix(match.group(1))
                        else:
                            prefix = match.group(1) + '/' + match.group(2)
                    if path != False:
                        cc_path = [catalog.get_asn(a) for a in path]
                        cc_prefix = catalog.get_pfx('ipv' + ipv, prefix)
                        writer.writerow([prefix, cc_prefix, " ".join(path), " ".join(cc_path)])
                else:
                    match = re.match(netre, line)
                    if match:
                        long = match.group(2)
                        if long is None:
                            prefix = addprefix(match.group(1))
                        else:
                            prefix = match.group(1) + '/' + match.group(2)


@click.command()
@click.option('--date', default='00000000', help='date of calculation')
@click.option('--ixp', default='aep', help='ixp identifier')
@click.option('--source', default='data', help='directory where the data is stored')
def main(date, ixp, source):
    if date == '00000000':
        date = datetime.today().strftime('%Y%m%d')

    year = date[0:4]
    month = date[4:6]
    day = date[6:8]

    catalog = ResourceCatalog()
    catalog.load_delegated("{dir}/delegated-{date}.csv".format(dir=source, date=date))

    with open('ixp-data.json') as json_file, open("{dir}/bgp-table-{ixp}-{date}.csv".format(dir=source, ixp=ixp, date=date), 'w', newline='') as fcsv:
        writer = csv.writer(fcsv)
        writer.writerow(["prefix", "prefix_cc", "as_path", "as_path_cc"])
        ixpdata = json.load(json_file)
        if ixp not in ixpdata:
            raise Exception("IXP not found")
        selected = ixpdata[ixp]

        if selected['source'] == 'pch':
            ipv = '4'
            url = "https://www.pch.net/resources/Routing_Data/IPv{ipv}_daily_snapshots/{year}/{month}/route-collector.{ixp}.pch.net/route-collector.{ixp}.pch.net-ipv{ipv}_bgp_routes.{year}.{month}.{day}.gz".format(ipv=ipv, year=year, month=month, day=day, ixp=ixp)
            process_pch(url, ipv, catalog, writer)
            ipv = '6'
            url = "https://www.pch.net/resources/Routing_Data/IPv{ipv}_daily_snapshots/{year}/{month}/route-collector.{ixp}.pch.net/route-collector.{ixp}.pch.net-ipv{ipv}_bgp_routes.{year}.{month}.{day}.gz".format(ipv=ipv, year=year, month=month, day=day, ixp=ixp)
            process_pch(url, ipv, catalog, writer)
        elif selected['source'] == 'lacnic':
            url = "https://ixpdata.labs.lacnic.net/raw-data/{path}/{y}/{m}/{d}/rib.{y}{m}{d}.{t}.bz2".format(path=selected['path'], y=year, m=month, d=day, t=selected['time'])
            stream = pybgpstream.BGPStream(data_interface="singlefile")
            stream.set_data_interface_option("singlefile", "rib-file", url)
            for rec in stream.records():
                for elem in rec:
                    prefix = elem.fields["prefix"]
                    if '.' in prefix:
                        v = 'ipv4'
                    else:
                        v = 'ipv6'
                    path = getpath(elem.fields["as-path"])
                    if path != False:
                        cc_path = [catalog.get_asn(a) for a in path]
                        cc_prefix = catalog.get_pfx(v, prefix)
                        writer.writerow([prefix, cc_prefix, " ".join(path), " ".join(cc_path)])


if __name__ == '__main__':
    main()
