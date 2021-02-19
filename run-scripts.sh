#!/bin/bash

TODAY=$(date +"%Y%m%d")
TS=${1:-$TODAY}
SECONDS=0

python download-delegated.py --date $TS
python process-ribs.py --date $TS
python process-ixp-data.py --date $TS
python process-country-data.py --date $TS
python process-prefix-data.py --date $TS
python get-routing-stats.py --date $TS
python process-as-data.py --date $TS

rm data/delegated-$TS.csv

echo "Elapsed time: $(($SECONDS / 60)) minutes"