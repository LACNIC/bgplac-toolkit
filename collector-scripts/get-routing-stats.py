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

    print("* Procesing routing stats")
    csm_df = pd.read_csv(source + "/country-summary-" + date + ".csv", index_col='country')
    pfx_df = pd.read_csv(source + "/prefix-summary-" + date + ".csv", index_col='country')
    ixp_df = pd.read_csv(source + "/ixp-summary-" + date + ".csv", index_col='country')

    pfc4 = pfx_df['prefix_count_ipv4']
    pfc6 = pfx_df['prefix_count_ipv6']

    frame = {
        'total_prefix_count': pfc4 + pfc6,
        'ipv4_prefix_count': pfc4,
        'ipv6_prefix_count': pfc6,
        'ipv4_prefix_length_mean': pfx_df['prefix_length_mean_ipv4'],
        'ipv6_prefix_length_mean': pfx_df['prefix_length_mean_ipv6'],
        'path_length_mean': (pfx_df['path_length_mean_ipv4'] * pfx_df['path_count_ipv4'] + pfx_df['path_length_mean_ipv6'] * pfx_df['path_count_ipv6']) / (pfx_df['path_count_ipv4'] + pfx_df['path_count_ipv6']),
        'origin_as_count': csm_df['total_origin_asns'],
        'transit_as_count': csm_df['total_transit_asns'],
        'upstream_as_count': csm_df['total_upstream_asns'],
        'unregistered_as_count': csm_df['total_unregistered_asns'],
        'offshore_as_count': csm_df['total_offshore_asns'],
        'ixp_count': ixp_df['ixp_count']
    }
    result = pd.DataFrame(frame)
    result.to_csv(source + "/country-routing-stats-" + date + ".csv", index_label='country', float_format='%.2f')
    print("- DONE!")


if __name__ == '__main__':
    main()
