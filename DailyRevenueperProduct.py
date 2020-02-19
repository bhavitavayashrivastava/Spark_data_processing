
'''
To determine the revenue of a product on daily basis .

three tables used orders,order_items,products

API used map,join,sortByKey,
'''






from pyspark import SparkConf,SparkContext

conf = SparkConf().setAppName("Daily Revenue Per Product").setMaster("local")

sc = SparkContext(conf=conf)

orders = sc.textFile("/sqoop_import/retail_db/orders")
orderItems = sc.textFile("/sqoop_import/retail_db/order_items")

ordersFiltered = orders.filter(lambda o: o.split(",")[3] in ["COMPLETE","CLOSED"])
ordersMap = ordersFiltered.map(lambda o: (int(o.split(",")[0]),o.split(",")[1]))

orderItemsMap = orderItems.map(lambda oi:(int(oi.split(",")[1]),(int(oi.split(",")[2]),float(oi.split(",")[4]))))
ordersJoin = ordersMap.join(orderItemsMap)
ordersJoinMap = ordersJoin.map(lambda o: ((o[1][0],o[1][1][0]),o[1][1][1]))
from operator import add
dailyRevenuePerProductId = ordersJoinMap.reduceByKey(add)

productsRaw = open("/home/hadoop/retail_db/products/part-00000").read().splitlines()
products = sc.parallelize(productsRaw)
productsMap = products.map(lambda p: (int(p.split(",")[0]),p.split(",")[2]))
dailyRevenuePerProductIdMap = dailyRevenuePerProductId.map(lambda r:(r[0][1],(r[0][0],r[1])))
dailyRevenuePerProductJoin = dailyRevenuePerProductIdMap.join(productsMap)
dailyRevenuePerProduct = dailyRevenuePerProductJoin.map(lambda t:((t[1][0][0],-t[1][0][1]),t[1][0][0]+ "," + str(t[1][0][1]) + "," + t[1][1]))
dailyRevenuePerProductSorted = dailyRevenuePerProduct.sortByKey()
dailyRevenueProductName = dailyRevenuePerProductSorted.map(lambda r: r[1])
dailyRevenueProductName.coalesce(2).saveAsTextFile("/user/hadoop/daily_revenue_txt_python")





### to run program with spark-submit 


#### spark-submit --master local src/main/python/DailyRevenuePerProduct.py 




