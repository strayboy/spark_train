import sys

from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: avg <input>", file=sys.stderr)
        sys.exit(-1)

    conf = SparkConf()
    sc = SparkContext(conf=conf)

    age_data = sc.textFile(sys.argv[1]).map(lambda x: x.split(" ")[1])
    total_age = age_data.map(lambda age: int(age)).reduce(lambda a, b: a + b)
    counts = age_data.count()
    avg_age = total_age / counts

    print(total_age)
    print(counts)
    print(avg_age)
    sc.stop()
