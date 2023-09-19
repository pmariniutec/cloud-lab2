from pyspark import SparkContext

if __name__ == "__main__":
    sc = SparkContext("local","PySpark Word Count")
    words = sc.textFile("data/input_file.csv").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a, b : a + b)

    output = wordCounts.collect()
    output.sort(key=lambda x: x[1], reverse=True)

    with open("data/output_file.txt", "w") as out:
        for (word, count) in output:
            out.write(f"({word}, {count})\n")
