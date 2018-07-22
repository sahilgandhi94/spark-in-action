from pyspark.sql import SparkSession

# create spark session 
spark = SparkSession.builder\
    .appName("Github push counter")\
    .master("local[*]")\
    .getOrCreate()

# spark context 
sc = spark.sparkContext

# load json file 
filePath = "/Users/sahil/data/githubarchive/2015-01-01-0.json"
ghLogs = spark.read.json(filePath)
print(f"type(ghLogs): {type(ghLogs)}")
print(f"all events: {ghLogs.count()}")

# filter ghLogs
pushes = ghLogs.filter("type = 'PushEvent'")
print(f"type(pushes): {type(pushes)}")
print(f"push events: {pushes.count()}")

# group based on login actor's count
grouped = pushes.groupBy("actor.login").count()
print(f"type(grouped): {type(grouped)}")

ordered = grouped.orderBy("count", ascending=False)
print(f"type(ordered): {type(ordered)}")

ordered.show(10)