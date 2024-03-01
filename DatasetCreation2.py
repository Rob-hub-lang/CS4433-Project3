import csv
import random
import string

# Function to generate a random string of characters
def random_string(length):
    return ''.join(random.choices(string.ascii_letters, k=length))

# Function to generate random data for Customers
def generate_customers(num_customers):

    with open('Customers.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['ID', 'Name', 'Age', 'CountryCode', 'Salary'])
        for i in range(1, num_customers + 1):
            writer.writerow([i, random_string(random.randint(10, 20)), 
                             random.randint(18, 100),
                             random.randint(1, 500),
                             round(random.uniform(100, 10000000), 2)])

def generate_purchases(num_purchases):

    num_customers = 50000 
    with open('Purchases.csv', 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['TransID', 'CustID', 'TransTotal', 'TransNumItems', 'TransDesc'])
        for i in range(1, num_purchases + 1):
            writer.writerow([i, random.randint(1, num_customers),
                             round(random.uniform(10, 2000), 2),
                             random.randint(1, 15),
                             random_string(random.randint(20, 50))])

# Generate data for 50,000 customers and 5,000,000 purchases
generate_customers(50000)
generate_purchases(5000000)
