#!/usr/bin/env python3


import click
import urllib.request
from datetime import datetime


@click.command()
@click.option('--date', default='00000000', help='date of calculation')
@click.option('--source', default='data', help='directory where the data is stored')
def main(date, source):
    if date == '00000000':
        date = datetime.today().strftime('%Y%m%d')
    url = "https://ftp.ripe.net/pub/stats/ripencc/nro-stats/" + date + "/combined-stat"
    print("* Downloading delegated from " + url)
    urllib.request.urlretrieve(url, source + "/delegated-" + date + ".csv")
    print("- DONE!")


if __name__ == '__main__':
    main()
