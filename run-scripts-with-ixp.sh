#!/bin/bash

TODAY=$(date +"%Y%m%d")
TS=${1:-$TODAY}
SECONDS=0

python download-delegated.py --date $TS
python collector-scripts/process-ribs.py --date $TS
python collector-scripts/process-ixp-data.py --date $TS
python collector-scripts/process-country-data.py --date $TS
python collector-scripts/process-prefix-data.py --date $TS
python collector-scripts/get-routing-stats.py --date $TS
python collector-scripts/process-as-data.py --date $TS

IXPSSTR=$(python get-active-ixps.py)
IXPS=$(echo $IXPSSTR | tr ";" "\n")

for IX in $IXPS
do
  python ixp-scripts/get-bgp-table.py --date $TS --ixp $IX --delegated-src data
  if [ $? -eq 0 ]
  then
    python ixp-scripts/process-bgp-table.py --date $TS --ixp $IX
    python ixp-scripts/process-coverage.py --date $TS --ixp $IX
    python ixp-scripts/process-ixp-summary.py --date $TS --ixp $IX
  else
    echo "! Skipping $IX"
    continue
  fi
done

echo "Elapsed time: $(($SECONDS / 60)) minutes"