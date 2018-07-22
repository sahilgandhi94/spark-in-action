from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import BooleanType

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

# filter only employees

# load employees in a set
empPath = "/Users/sahil/Projects/github.com/spark-in-action/ch03/ghEmployees.txt"
employees = {emp.strip() for emp in open(empPath).readlines()}
print(f'employee count: {len(employees)}')
bcEmployees = sc.broadcast(employees) # broadcase the `employees` var
print(f"type(bcEmployees): {type(bcEmployees)}")

isEmp = lambda user: user in bcEmployees.value
isEmpUdf = spark.udf.register("SetContainsUdf", isEmp, BooleanType())
print(f'type(isEmpUdf): {type(isEmpUdf)}')

filteredEmployees = ordered.filter(isEmpUdf(col("login")))
filteredEmployees.show()
