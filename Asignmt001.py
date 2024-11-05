import os
from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.sql.functions import col, when, avg


os.environ["PYSPARK_PYTHON"] = "C:/Users/Admin/AppData/Local/Programs/Python/Python37/python.exe"  # or "python" depending on your Python executable


sc = SparkContext("local[4]", "sparkrdd")
arr = [10,20,30,40,50,60,70,80,90]
rdd1 = sc.parallelize(arr)
avg=rdd1.mean()
print("output" ,avg)


rdd = sc.parallelize([1, 2, 3, 4, 5])
total_sum = rdd.reduce(lambda x, y: x + y)
count = rdd.count()
average = total_sum / count
print("output of avg",average)
