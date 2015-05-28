import sys
import optparse
from pyspark import SparkContext

class SparkCacheExample:
    """
    A class to illustrate usage of some the Spark's APIs namely:
    - textFile(), cache(), filter(), collect()
    """

    def __init__(self, inputFile, shouldCache):
        self.inputFile = inputFile
        self.inputRdd = None
        self.shouldCache = shouldCache

    def __call__(self):
        sc = SparkContext("local", "Simple App")
        self.inputRdd = sc.textFile(self.inputFile)
        if self.shouldCache:
            self.inputRdd = self.inputRdd.cache()
        return self

    def printCountLines(self):
        print "Lines Count: " + str(self.inputRdd.count())
        return self
        
    def printFilteredLines(self, inputFilter):
        filteredRdd = self.inputRdd.filter(lambda line: inputFilter in line)
        print "Lines containing (" + inputFilter + "):"
        for line in filteredRdd.collect():
            print line
        return self

if __name__ == "__main__":
    #
    # Submit this program using the following command:
    # $SPARK_HOME/bin/spark-submit SparkCacheExample.py
    #
    parser = optparse.OptionParser(usage='%prog [options]')
    parser.add_option('--cache', action="store_true", dest='shouldCache', default=False)
    parser.add_option('--input', action="store", dest='input', default=None)
    parser.add_option('--filter', action="append", dest='filters', default=[])

    options, remainder = parser.parse_args()
    print options
    inputFile = "/etc/hosts" if options.input == None else options.input
    shouldCache = options.shouldCache
    filters = options.filters
    obj = SparkCacheExample(inputFile, shouldCache)
    obj()

    obj.printCountLines()
    for filter in filters:
        obj.printFilteredLines(filter)
