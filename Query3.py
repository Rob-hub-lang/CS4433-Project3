from pyspark import SparkContext, SparkConf

# Initialize Spark
conf = SparkConf().setAppName("CloseContacts").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Function to calculate close contacts count for a person
def calc_close_contacts_count(person, infected_people):
    infect_i_id = person[0]
    close_contacts_count = 0
    for infected_person in infected_people:
        if infect_i_id != infected_person[0]:  # Exclude infect-i itself
            # Calculate Euclidean distance
            distance = ((person[1] - infected_person[1])**2 + (person[2] - infected_person[2])**2)**0.5
            # Consider as close contact if distance <= 6
            if distance <= 6:
                close_contacts_count += 1
    return close_contacts_count

# Read data from file
psil_rdd = sc.textFile("PEOPLE-SOME-INFECTED-large.csv") \
    .map(lambda line: line.split(",")).map(lambda x: (int(x[0]), int(x[1]), int(x[2]), x[3], int(x[4]), x[5]))

# Identify infected people
infected_people_rdd = psil_rdd.filter(lambda x: x[5] == 'yes')

# Count total number of infected people
total_infected_count = infected_people_rdd.count()
print(f"Total number of infected people: {total_infected_count}")

# Create a broadcast variable with the infected people IDs
infected_ids_broadcast = sc.broadcast(infected_people_rdd.collect())

# Calculate close contacts counts for each infected person
close_contacts_counts_rdd = infected_people_rdd.map(lambda x: (x[0], calc_close_contacts_count(x, infected_ids_broadcast.value)))

# Print the results
for result in close_contacts_counts_rdd.collect():
    print(f"Infected Person {result[0]} has {result[1]} close contacts within 6 feet of other people")

# Stop SparkContext
sc.stop()
