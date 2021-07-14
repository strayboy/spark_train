from pyspark import SparkContext, SparkConf

if __name__ == '__main__':
    conf = SparkConf().setMaster("local[2]").setAppName("spark0401")
    sc = SparkContext(conf=conf)


    def my_map():
        data = [1, 2, 3, 4, 5]
        rdd1 = sc.parallelize(data)
        rdd2 = rdd1.map(lambda x: x * 2)
        print(rdd2.collect())


    def my_map2():
        a = sc.parallelize(["dog", "tiger", "lion", "cat", "panther", "eagle"])
        b = a.map(lambda x: (x, 1))
        print(b.collect())


    def my_filter():
        data = [i for i in range(1, 6)]
        rdd1 = sc.parallelize(data)
        map_rdd = rdd1.map(lambda x: x * 2)
        filter_rdd = map_rdd.filter(lambda x: x > 5)
        # print(filter_rdd.collect())
        print(sc.parallelize(data).map(lambda x: x * 2).filter(lambda x: x > 5).collect())


    def my_flatmap():
        data = ["hello spark", "hello world", "hello world"]
        rdd = sc.parallelize(data)
        print(rdd.flatMap(lambda line: line.split(" ")).collect())


    def my_groupBy():
        data = ["hello spark", "hello world", "hello world"]
        rdd = sc.parallelize(data)
        split__map = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
        key = split__map.groupByKey()
        print(key.collect())
        print(key.map(lambda x: {x[0]: list(x[1])}).collect())


    def my_reduceByKey():
        data = ["hello spark", "hello world", "hello world"]
        rdd = sc.parallelize(data)
        split__map = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
        key = split__map.reduceByKey(lambda a, b: a + b)
        print(key.collect())


    def my_sortByKey():
        data = ["hello spark", "hello world", "hello world"]
        rdd = sc.parallelize(data)
        split__map = rdd.flatMap(lambda line: line.split(" ")).map(lambda x: (x, 1))
        key = split__map.reduceByKey(lambda a, b: a + b)
        by_key = key.sortByKey()
        print(by_key.collect())
        print(key.map(lambda x: (x[1], x[0])).collect())
        print(key.map(lambda x: (x[1], x[0])).sortByKey(False).collect())
        print(key.map(lambda x: (x[1], x[0])).sortByKey(False).map(lambda x: (x[1], x[0])).collect())


    def my_union():
        a = sc.parallelize([1, 2, 3])
        b = sc.parallelize([3, 4, 5])
        print(a.union(b).collect())


    def my_distinct():
        a = sc.parallelize([1, 2, 3])
        b = sc.parallelize([3, 4, 2])
        collect = a.union(b).distinct().collect()
        print(collect)


    def my_join():
        a = sc.parallelize([("A", "a1"), ("C", "c1"), ("D", "d1"), ("F", "f1"), ("F", "f2")])
        b = sc.parallelize([("A", "a2"), ("C", "c2"), ("C", "c3"), ("E", "e1")])

        a.join(b).collect()


    def my_action():
        data = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        rdd = sc.parallelize(data)
        rdd.collect()
        rdd.reduce(lambda x, y: x + y)

        rdd.foreach(lambda x: print(x))


    my_sortByKey()
    sc.stop()
