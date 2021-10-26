#!/bin/bash

TODAY=$(date +"%Y%m%d")
TS=${1:-$TODAY}
SECONDS=0

#python download-delegated.py --date $TS
#python collector-scripts/process-ribs.py --date $TS
#python collector-scripts/process-ixp-data.py --date $TS
#python collector-scripts/process-country-data.py --date $TS
#python collector-scripts/process-prefix-data.py --date $TS
#python collector-scripts/get-routing-stats.py --date $TS
#python collector-scripts/process-as-data.py --date $TS

for IX in aep ari asu gye scl sjo tgu crix ixpgt ixsy ixpy; do
  python ixp-scripts/get-bgp-table.py --date $TS --ixp $IX --delegated-src data
  python ixp-scripts/process-bgp-table.py --date $TS --ixp $IX
  python ixp-scripts/process-coverage.py --date $TS --ixp $IX
  python ixp-scripts/process-ixp-summary.py --date $TS --ixp $IX
done

echo "Elapsed time: $(($SECONDS / 60)) minutes"