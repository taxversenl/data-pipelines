import os
import csv
import random
from datetime import datetime, timedelta

# Generate random sample data for 1 million records
def generate_data(num_records):
    data = []
    for _ in range(num_records):
        customer_id = random.randint(1000, 9999)
        name = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=10))
        transaction_id = ''.join(random.choices('0123456789abcdef', k=8))  # Generate a random transaction ID
        invoice_number = f"INV-{datetime.now().strftime('%Y%m%d')}-{customer_id}"
        invoice_date = (datetime.now() - timedelta(days=random.randint(1, 365))).strftime('%Y-%m-%d')
        invoice_amount = round(random.uniform(100, 5000), 2)
        currency = random.choice(["USD", "EUR", "GBP"])
        data.append({"TransactionID": transaction_id, "CustomerID": customer_id, "Name": name, "InvoiceNumber": invoice_number, "InvoiceDate": invoice_date, "InvoiceAmount": invoice_amount, "Currency": currency})
    return data

# Create directory if it doesn't exist
directory = "AR"
if not os.path.exists(directory):
    os.makedirs(directory)

# CSV file path
csv_file_path = os.path.join(directory, "ar.csv")

# CSV column headers
headers = ["TransactionID", "CustomerID", "Name", "InvoiceNumber", "InvoiceDate", "InvoiceAmount", "Currency"]

# Generate data
data = generate_data(1000000)  # Generate 1 million records

# Write data to CSV file
with open(csv_file_path, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=headers)
    
    # Write header
    writer.writeheader()
    
    # Write rows
    for row in data:
        writer.writerow(row)

print(f"CSV file generated successfully in directory: {directory}!")
