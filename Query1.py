from pyspark import SparkContext, SparkConf

# Create SparkContext
c = SparkConf().setAppName("Q1").setMaster("local[*]")
sc = SparkContext(conf=c)

# Load data
p_rdd = sc.textFile("PEOPLE-large.csv")
i_rdd = sc.textFile("INFECTED-small.csv")

# Parse data
p_parsed_rdd = p_rdd.map(lambda line: line.split(",")).map(lambda x: (int(x[0]), int(x[1]), int(x[2]), x[3], int(x[4])))
i_parsed_rdd = i_rdd.map(lambda line: line.split(",")).map(lambda x: (int(x[0]), int(x[1]), int(x[2]), x[3], int(x[4])))

# Broadcast infected data
i_broadcast = sc.broadcast(i_parsed_rdd.collect())

# Calculate close contacts
c_rdd = p_parsed_rdd.flatMap(lambda x: [(x[0], y[0]) for y in i_broadcast.value if ((x[1]-y[1])**2 + (x[2]-y[2])**2)**0.5 <= 6])

# Collect the results
result = c_rdd.collect()

# Print the results with labels
print("Close Contacts:")
for pair in result:
    print(f"Person {pair[0]} is in close contact with Infected Person {pair[1]}")
# Stop SparkContext
sc.stop()
