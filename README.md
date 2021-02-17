# BGP Interconnection Data Toolkit

Scripts used to process data used in LACNIC's repport: 
[BGP Interconnection in the Region of Latin America and the Caribbean](https://www.lacnic.net/innovaportal/file/4298/1/lacnic-interconexion-bgp-lac-en.pdf).

## Installation

Install dependencies:

* pytricia
* pandas

## Usage

Data is processed through a series of scripts, to divide the problem in smaller chuncks of code.
Make a copy of `run-scripts.sh` and modify it to get the expected result.

![Data flow](data-flow.png?raw=true "Data flow")

### Scripts

`process-ribs.py`
This script has the following parameters:
* date: The script will process data from that date (YYYYMMDD format). Default value: current date.
* collectors: Collectors to use as data source. Multiple collectors can be selected with comma separated values (Ex. --collectos rrc00,rrc06) Default value: rrc00.
* region: Process contries from selected region (afrinic, apnic, arin, lacnic or ripencc). Default value: lacnic.
* source: directory to save created datasets. Default value: data.

`download delegated.py`
This script has the following parameters:
* date: The script will process data from that date (YYYYMMDD format). Default value: current date.
* source: Directory to save and load datasets. Default value: data.

`process-ixp-data.py`
This script has the following parameters:
* date: The script will process data from that date (YYYYMMDD format). Default value: current date.
* source: Directory to save and load datasets. Default value: data.

`process-country-data.py`
This script has the following parameters:
* date: The script will process data from that date (YYYYMMDD format). Default value: current date.
* source: Directory to save and load datasets. Default value: data.

`process-prefix-data.py`
This script has the following parameters:
* date: The script will process data from that date (YYYYMMDD format). Default value: current date.
* source: Directory to save and load datasets. Default value: data.

`get-routing-stats.py`
This script has the following parameters:
* date: The script will process data from that date (YYYYMMDD format). Default value: current date.
* source: Directory to save and load datasets. Default value: data.

`process-as-data.py`
This script has the following parameters:
* date: The script will process data from that date (YYYYMMDD format). Default value: current date.
* source: Directory to save and load datasets. Default value: data.
* region: Process countries from selected region (afrinic, apnic, arin, lacnic or ripencc). Default value: lacnic.

### Datasets

`country-data-<t>.csv`
Lists  the  ASNs for  each  country considering their classification (origin,  transit  or  upstream) and separates them based on the IP protocol.

`prefix-data-<t>.csv`
Specifies data for each prefix announced and registered at LACNIC, including the country and origin ASN as well as the number of routes and total hops.

`as-data-<t>.csv`
Records data for each AS in the region, as well as the prefixes they announce and their relationship with other autonomous systems.

`country-summary-<t>.csv`
Summarizes the information in country-data-<t>.csv, specifying the number of autonomous systems by country according to their classification (origin, transit or upstream)

`prefix-summary-<t>.csv`
Groups the data included in prefix-data-<t>.csv by country, which allows obtaining general data for each one, such as average prefix and AS path length, total number of prefixes announced, etc.

`ixp-summary-<t>.csv`
Specifies the number of Internet exchange points in each country.

`country-routing-stats-<t>.csv`
Groups the information in country-summary-<t>.csv, prefix-summary-<t>.csv and ixp-data-summary-<t>.csv to obtain, for each country, a set of relevant data related to its Internet development.

## Acknowledgments

* Author: Augusto Mathurin [@augusthur](https://twitter.com/augusthur)
* Coordination: Guillermo Cicileo

Strengthening Regional Internet Infrastructure
LACNIC

## License

[MIT](https://choosealicense.com/licenses/mit/)

