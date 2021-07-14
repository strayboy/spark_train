import sys

from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: TopN <input>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    counts = sc.textFile(sys.argv[1]).map(lambda x: x.split("\t")).map(lambda x: (x[5], 1)).reduceByKey(
        lambda a, b: a + b).map(lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0])).take(5)

    for word, cnt in counts:
        print("{}: {}".format(word, cnt))
    sc.stop()
