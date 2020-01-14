'''
To find out Airpors in Usa and filter out from the dataset using Pyspark 
dataset used "airports.text"
'''

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster('local').setAppName("Airport")

sc = SparkContext(conf=conf)


def clean_data(x):
    bad_chars = ['"']
    return ''.join(i for i in x if not i in bad_chars)


airports = sc.textFile("file:///home/hadoop/data/airports.text")

airportsFiltered = airports.filter(lambda port: clean_data(port).split(",")[3]== "United States")

airportsMap = airportsFiltered.map(lambda port: (clean_data(port).split(",")[1],clean_data(port).split(",")[2]))

airportsMap.saveAsTextFile("airportsName")
