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
        tax_registration_id = "NL000099998B57"  # Set tax registration ID
        dataset_type = "AR"  # Set dataset type as "AR"
        reporting_period = "05-2024"  # Set reporting period as "05-2024"
        data.append({
            "tax_registration_id": tax_registration_id,
            "dataset_type": dataset_type,
            "reporting_period": reporting_period,
            "transaction_id": transaction_id,
            "customer_id": customer_id,
            "name": name,
            "invoice_number": invoice_number,
            "invoice_date": invoice_date,
            "invoice_amount": invoice_amount,
            "currency": currency
        })
    return data

# Create directory if it doesn't exist
directory = "AR"
if not os.path.exists(directory):
    os.makedirs(directory)

# CSV file path
csv_file_path = os.path.join(directory, "ar.csv")

# CSV column headers in snake case
snake_case_headers = ["tax_registration_id", "dataset_type", "reporting_period", "transaction_id", "customer_id", "name", "invoice_number", "invoice_date", "invoice_amount", "currency"]

# Generate data
data = generate_data(1000000)  # Generate 1 million records

# Write data to CSV file
with open(csv_file_path, mode="w", newline="", encoding="utf-8") as file:
    writer = csv.DictWriter(file, fieldnames=snake_case_headers)
    
    # Write header
    writer.writeheader()
    
    # Write rows
    for row in data:
        writer.writerow(row)

print(f"CSV file generated successfully in directory: {directory}!")
