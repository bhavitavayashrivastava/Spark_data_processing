'''Create a Spark program to read the airport data from "airports.text",
    find all the airports whose latitude are bigger than 40.
    Then output the airport's name and the airport's latitude to airports_by_latitude.text.
    Each row of the input file contains the following columns:
    Airport ID, Name of airport, Main city served by airport, Country where airport is located, IATA/FAA code,
    ICAO Code, Latitude, Longitude, Altitude, Timezone, DST, Timezone in Olson format


Sample output:
    "St Anthony", 51.391944
    "Tofino", 49.082222
    
    '''


rom pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster('local').setAppName('Airport latitude')

sc = SparkContext(conf=conf)


def remove_record(x):
    if x.split(",")[6].isalpha():
        pass
    else:
        return x


def rm_char(x):
    bad_chars = ['"']
    return ''.join(i for i in x if not i in bad_chars)


def clean_data(x):
    bad_chars = ['"']
    return remove_record(''.join(i for i in x if not i in bad_chars))


airports = sc.textFile("file:///home/hadoop/data/airports.text")

airportsFiltered = airports.filter(lambda port: clean_data(port) != None)

airportsReFiltered = airportsFiltered.filter(lambda port: float(port.split(",")[6]) > 40)

airportsMap = airportsReFiltered.map(lambda m: (rm_char(m).split(",")[1], m.split(",")[6]))

# print(airportsMap.first())  (for validation I used this statement)

airportsMap.saveAsTextFile("airportsLattitude")

