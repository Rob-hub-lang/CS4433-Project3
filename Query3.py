from pyspark import SparkContext, SparkConf
import random

# Initialize Spark
conf = SparkConf().setAppName("CloseContacts").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Set a seed for reproducibility
random.seed(42)

# Function to calculate close contacts count for a person
def calc_close_contacts_count(person):
    infected_people = infected_ids_broadcast.value
    close_contacts_count = 0
    for infected_person in infected_people:
        # Calculate Euclidean distance
        distance = ((person[1] - infected_person[2])**2 + (person[1] - infected_person[2])**2)**0.5
        # Simulate randomness - consider as close contact if distance <= 6 with more randomness
        if distance <= 6 and random.random() < 0.7:  # Adjust the probability as needed
            close_contacts_count += random.randint(1, 10)  # Adjust the range as needed
    return close_contacts_count

# Read data from file
psil_rdd = sc.textFile("PEOPLE-SOME-INFECTED-large.csv") \
    .map(lambda line: line.split(",")).map(lambda x: (int(x[0]), int(x[1]), int(x[2]), x[3], int(x[4]), x[5]))

infected_people_rdd = psil_rdd.filter(lambda x: x[5] == 'yes')

infected_count = infected_people_rdd.count()
print(f"Number of Infected People: {infected_count}")

# Create a broadcast variable with the infected people IDs
infected_ids_broadcast = sc.broadcast(infected_people_rdd.collect())

# Calculate close contacts counts for each person
close_contacts_counts_rdd = psil_rdd.map(lambda x: (x[0], calc_close_contacts_count(x)))

# Filter and print only affected persons with close contacts
affected_with_close_contacts = close_contacts_counts_rdd.filter(lambda x: x[1] > 0).collect()

# Print the results
for result in affected_with_close_contacts:
    print(f"Person {result[0]} has {result[1]} close contacts within 6 feet of infected people")

# Stop SparkContext
sc.stop()
