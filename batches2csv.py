import sys
sys.path.append('/mnt/g/My Drive/Computer/python/breeze/pyBreezeChMS')
#sys.path.append('/mnt/y/My Drive/Computer/python/breeze/pyBreezeChMS')
from pyBreezeChMS.breezeapi import get_batches, contributions_with_addresses
import argparse
from makeletters import save

def parse_range(s):
    items = s.split(',')
    range_list = []
    for item in items:
        if '-' in item:
            start, end = item.split('-')
            range_list.extend(list(range(int(start), int(end)+1)))
        else:
            range_list.append(int(item))
    return range_list

def makefilename(batchlist):
    filename = "batches_"
    for i in range(len(batchlist)):
        if i < len(batchlist) - 1:
            filename += str(batchlist[i])+"-"
        else:
            filename += str(batchlist[i])+".csv"
    return filename   

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('range', type=parse_range, help='Range to process')
    args = parser.parse_args()
    batch_data = get_batches(args.range)
    contributions_for_letters = contributions_with_addresses(batch_data)
    filename = makefilename(args.range)
    save(contributions_for_letters, filename)
