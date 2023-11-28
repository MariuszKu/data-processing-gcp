from faker import Faker
import random
import datetime
import pandas as pd
import os

fake = Faker()

PROJECT = os.environ['PROJECT']
LANDING = os.environ['LANDING']

# Different types of credit cards and their offers
credit_card_types = [
    {"type": "Standard", "credit_limit": 1000, "annual_fee": 0},
    {"type": "Gold", "credit_limit": 5000, "annual_fee": 50},
    {"type": "Platinum", "credit_limit": 10000, "annual_fee": 100}
]

# Generate synthetic client data 
def generate_client_data(num_clients=100000):
    clients = []
    for client_num in range(1, num_clients + 1):
        client = {
            "client_number": client_num,
            "name": fake.name(),
            "email": fake.email(),
            "phone_number": fake.phone_number(),
            "bulding_number": fake.building_number(),
            "street_name": fake.street_name(),
            "postcode": fake.postcode(),
            "city": fake.city(),
            "state": fake.state(),
            "birth_date": fake.date_of_birth(minimum_age=18, maximum_age=90).strftime('%Y-%m-%d'),
            "credit_card_number" : fake.credit_card_number(card_type='mastercard'),
        }
        clients.append(client)
    return clients

# Generate synthetic client application data 
def generate_client_applications(clients):
    states = ["CA", "NY", "TX", "FL", "IL", "PA", "OH", "GA", "NC", "MI"]
    applications = []
    for client in clients:
        agreement_assignment_date = fake.date_between(start_date="-3y")
        application_date = fake.date_between(
            start_date=agreement_assignment_date - datetime.timedelta(days=90), 
            end_date=agreement_assignment_date
            )
        decision_date = fake.date_between(
            start_date=application_date - datetime.timedelta(days=90),
            end_date=application_date)

        status = random.choice(["approved", "pending", "rejected"])
        application_number = fake.unique.uuid4()
        client["credit_card_number"] = client["credit_card_number"] if status == "approved" else ''

        application = {
            "application_number": application_number,
            "client_number": client["client_number"],
            "client_name": client["name"],
            "client_email": client["email"],
            "application_date": application_date.strftime('%Y-%m-%d'),
            "status": status,
            "credit_card_type": random.choice(credit_card_types)["type"],
            "agreement_assignment_date": agreement_assignment_date.strftime('%Y-%m-%d'),
            "application_date": application_date.strftime('%Y-%m-%d'),
            "decision_date": decision_date.strftime('%Y-%m-%d'),
            "branch": random.choice(states)
        }
        applications.append(application)
    return applications, clients

# Generate synthetic card operations data for approved applications
def generate_card_operations(clients, applications):
    card_operations = []
    for client, application in zip(clients, applications):
        if application["status"] == "approved":
            num_transactions = random.randint(0, 300)
            for _ in range(num_transactions):
                transaction_id = fake.unique.uuid4()
                start_date = datetime.datetime.strptime(application["application_date"], '%Y-%m-%d')
                tran_date = fake.date_between(
                    start_date=start_date,
                    end_date=datetime.timedelta(days=1)
                    )
                operation = {
                    "transaction_id": transaction_id,
                    "client_num": client["client_number"],
                    "card_number": client["credit_card_number"],
                    "transaction_amount": fake.random_int(min=1, max=100000)/10,
                    "transaction_date": tran_date.strftime('%Y-%m-%d'),
                    "merchant": fake.company(),
                    "status": random.choice(
                        ["approved", "approved","approved","approved","approved","approved","approved","approved","approved","approved","declined"]
                        )
                }
                card_operations.append(operation)
    return card_operations


def branch():

    states = [ {"branch": "CA", "manger" :fake.name() } , 
              {"branch":"NY", "manger" :fake.name()}, 
              {"branch":"TX", "manger" :fake.name()}, 
              {"branch":"FL", "manger" :fake.name()}, 
              {"branch":"IL", "manger" :fake.name()}, 
              {"branch":"PA", "manger" :fake.name()},
              {"branch": "OH", "manger" :fake.name()}, 
              {"branch":"GA", "manger" :fake.name()}, 
              {"branch":"NC", "manger" :fake.name()}, 
              {"branch":"MI", "manger" :fake.name()}
                ]
    
    df = pd.DataFrame(states)
    df.to_excel(f"gs://{LANDING}/branch.xlsx")



def main():
    # Generate client data 
    clients = generate_client_data()
    print("Generated Client Data")
    # Generate client application data 
    client_applications, clients = generate_client_applications(clients)
    print(client_applications[10])
    # Generate card operations data for approved applications
    card_operations = generate_card_operations(clients, client_applications)
    print("Generated card_operations")
    # Print generated data
    
    df = pd.DataFrame(clients)
    df.to_csv(f"gs://{LANDING}/clients.csv", index=False)

    print("\nGenerated Client Applications")
    df = pd.DataFrame(client_applications)
    df.to_csv(f"gs://{LANDING}/client_applications.csv", index=False)

    print("\nGenerated Card Operations Data")
    df = pd.DataFrame(card_operations)
    df.to_csv(f"gs://{LANDING}/card_operations.csv", index=False)


if __name__ == "__main__":
    main()
    branch()