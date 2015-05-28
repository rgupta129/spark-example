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
    inputFile = "/etc/hosts"
    shouldCache = True
    obj = SparkCacheExample(inputFile, shouldCache)
    obj().printCountLines().printFilteredLines("9").printFilteredLines("com").printCountLines().printFilteredLines("net")
