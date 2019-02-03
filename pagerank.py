from __future__ import print_function

import re
import sys
from operator import add

from pyspark import SparkContext


def urlweights(urls, rank): #weightage for each url jump or link
    """ URL Weights. """
    total_urls = len(urls)
    for url in urls:
        yield (url, rank / total_urls)


def pairs(urls): #get the links from the input file
    """Form Key-Value Pairs with the URLs."""
    parts = re.split(r'\s+', urls)
    return parts[0], parts[1]


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Invalid Entries, should be 3", file=sys.stderr)
        exit(1)

    sc = SparkContext(appName="PageRank")

    lines = sc.textFile(sys.argv[1], 1)

    """Get all pairs of URLs and group by similar key"""
    URLs = lines.map(lambda urls: pairs(urls)).distinct().groupByKey()
    URLs.cache()

    """Intialize all key-value pairs to 1 as a starting point"""
    ranks = URLs.map(lambda url_connected: (url_connected[0], 1.0))

    """No. of iterations to assume"""
    for iteration in range(int(sys.argv[2])):
        """Essentially divide every value to the key it goes and count"""
        urls_joined = URLs.join(ranks).flatMap(
            lambda urls_rank: urlweights(urls_rank[1][0], urls_rank[1][1]))

        """ Taking care of spider traps. This is actually a very crude form of damping, Its normally d=.85 but I've assumed .80"""
        ranks = urls_joined.reduceByKey(add).mapValues(lambda rank: rank * 0.80 + 0.20)

    """Collect and Save to file"""
    for (url, rank) in ranks.collect():
        linkrank = str(str(url)+"    "+str(rank)) + "\n"
        with open("/outputdirectory","a+cd") as myfile:
            myfile.write(str(linkrank))

sc.stop()
