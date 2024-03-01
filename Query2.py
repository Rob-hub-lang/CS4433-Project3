from pyspark import SparkContext, SparkConf

# Create SparkContext
conf = SparkConf().setAppName("Q2").setMaster("local[*]")
sc = SparkContext(conf=conf)

# Load data
p_rdd = sc.textFile("PEOPLE-large.csv")
i_rdd = sc.textFile("INFECTED-small.csv")

# Parse data
p_parsed_rdd = p_rdd.map(lambda line: line.split(",")).map(lambda x: (int(x[0]), int(x[1]), int(x[2]), x[3], int(x[4])))
i_parsed_rdd = i_rdd.map(lambda line: line.split(",")).map(lambda x: (int(x[0]), int(x[1]), int(x[2]), x[3], int(x[4])))

# Broadcast infected data
i_broadcast = sc.broadcast(i_parsed_rdd.collect())

# Calculate close contacts and get distinct IDs
distinct_close_contacts_rdd = p_parsed_rdd.flatMap(lambda x: [x[0] for y in i_broadcast.value if ((x[1]-y[1])**2 + (x[2]-y[2])**2) <= 6**2]) \
                                        .distinct()

# Collect the results
result = distinct_close_contacts_rdd.collect()

# Print the results with labels
print("Distinct IDs of People with Close Contacts to Infected Persons:")
for person_id in result:
    print(f"Person {person_id} had at least one close contact with an infected person")

# Stop SparkContext
sc.stop()
