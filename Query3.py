from pyspark import SparkContext, SparkConf
import random
import logging

conf = SparkConf().setAppName("Q3").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Function to calculate close contacts count for a person
def calc_close_contacts_count(person):
    infected_people = infected_ids_broadcast.value
    close_contacts_count = 0
    for infected_id in infected_people:
        if person[0] != int(infected_id) and ((person[1]-int(infected_id))**2 + (person[2]-int(infected_id))**2)**0.5 <= 6:
            close_contacts_count += 1
    return close_contacts_count


psil_rdd = sc.textFile("PEOPLE-SOME-INFECTED-large.csv") \
    .map(lambda line: line.split(",")).map(lambda x: (int(x[0]), int(x[1]), int(x[2]), x[3], int(x[4]), x[5]))


infected_people_rdd = psil_rdd.filter(lambda x: x[5] == 'yes')


infected_count = infected_people_rdd.count()
print(f"Number of Infected People: {infected_count}")


# Create a broadcast variable with the infected people IDs
infected_ids_broadcast = sc.broadcast(infected_people_rdd.map(lambda x: x[0]).collect())

# Calculate close contacts counts for each infected person
close_contacts_counts_rdd = infected_people_rdd.map(lambda x: (x[0], calc_close_contacts_count(x)))

# Collect the results
results = close_contacts_counts_rdd.collect()

# Print the results
for result in results:
    print(f"Infected Person {result[0]}: {result[1]} close contacts within 6 feet")

# Stop SparkContext
sc.stop()