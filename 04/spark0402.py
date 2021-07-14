import sys

from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: wordcount <input> <output>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)


    def printResult():
        counts = sc.textFile(sys.argv[1]).flatMap(lambda line: line.split("\t")).map(lambda x: (x, 1)).reduceByKey(
            lambda a, b: a + b)

        output = counts.collect()
        for word, cnt in output:
            print("{}: {}".format(word, cnt))
        counts.cache()

    def save_file():
        sc.textFile(sys.argv[1]).flatMap(lambda line: line.split("\t")).map(lambda x: (x, 1)).reduceByKey(
            lambda a, b: a + b).saveAsTextFile(sys.argv[2])


    save_file()
    sc.stop()
