'''


    Create a Spark program to read the airport data from in/airports.text, find all the airports which are located in United States
    and output the airport's name and the city's name to out/airports_in_usa.text.
    Each row of the input file contains the following columns:
    
    
    
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format
    Sample output:
    "Putnam County Airport", "Greencastle"
    "Dowagiac Municipal Airport", "Dowagiac"
    



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

airportsMap.saveAsTextFile("airportsNameAndCity")
