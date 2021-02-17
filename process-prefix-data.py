#!/usr/bin/env python3


import click
import csv
import pandas as pd
from datetime import datetime


@click.command()
@click.option('--date', default='00000000', help='date of calculation')
@click.option('--source', default='data', help='directory where the data is stored')
def main(date, source):
    if date == '00000000':
        date = datetime.today().strftime('%Y%m%d')
    inpath = source + "/prefix-data-" + date + ".csv"
    print("* Procesing prefixes from " + inpath)
    pfx_df = pd.read_csv(inpath)
    gr = pfx_df.groupby(['country', 'version'])
    frame = {
        'prefix_count': gr['prefix'].count(),
        'prefix_length_mean': gr['length'].mean(),
        'prefix_length_std': gr['length'].std(),
        'path_length_mean': gr['jumps'].sum() / gr['paths'].sum(),
        'path_count': gr['paths'].sum()
    }
    df = pd.DataFrame(frame)
    dipv4 = df.xs('ipv4', level='version')
    dipv6 = df.xs('ipv6', level='version')
    result = pd.merge(dipv4, dipv6, on='country', suffixes=["_ipv4", "_ipv6"], how='outer')
    result.to_csv(source + "/prefix-summary-" + date + ".csv", index_label='country', float_format='%.2f')
    print("- DONE!")


if __name__ == '__main__':
    main()
