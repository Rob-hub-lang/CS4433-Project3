from pyspark import SparkContext
import random

# Create a Spark context
sc = SparkContext.getOrCreate()

# create people-large dataset
people_large = 10000
people_large_rdd = sc.parallelize(range(people_large)) \
    .map(lambda id: f"{id + 1},{random.randint(1, 10000)},{random.randint(1, 10000)},Person_{id + 1},{random.randint(18, 80)}")
people_large_rdd.saveAsTextFile("PEOPLE-large.csv")

# create infected-small dataset
infected_small = 100
infected_ids = random.sample(range(1, people_large + 1), infected_small)
infected_small_rdd = sc.parallelize(infected_ids) \
    .map(lambda id: f"{id},{random.randint(1, 10000)},{random.randint(1, 10000)},Person_{id},{random.randint(18, 80)}")
infected_small_rdd.saveAsTextFile("INFECTED-small.csv")

# create people-some-infected-large dataset
people_some_infected_large = 10000
people_some_infected_large_rdd = sc.parallelize(range(people_some_infected_large)) \
    .map(lambda id: f"{id + 1},{random.randint(1, 10000)},{random.randint(1, 10000)},Person_{id + 1},{random.randint(18, 80)},{ 'yes' if id + 1 in infected_ids else 'no'}")
people_some_infected_large_rdd.saveAsTextFile("PEOPLE-SOME-INFECTED-large.csv")

sc.stop()
