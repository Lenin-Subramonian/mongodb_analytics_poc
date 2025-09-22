# setup_mongodb_data.py
from pymongo import MongoClient
from datetime import datetime, timedelta
import random

def create_sample_data():
    # Connect to MongoDB
    # client = MongoClient('mongodb://localhost:27017/')
    uri = "mongodb+srv://ls_db_user:s47tBnWikllAoe3k@democluster0.j777gry.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster0"
    client = MongoClient(uri)
    db = client['ecommerce_db']
    collection = db['orders']
    
    # Sample data generation
    products = ['laptop', 'mouse', 'keyboard', 'monitor', 'tablet']
    customers = ['alice@email.com', 'bob@email.com', 'charlie@email.com', 'diana@email.com']
    
    sample_orders = []
    for i in range(1000):
        order = {
            'order_id': f'ORD-{i+1:06d}',
            'customer_email': random.choice(customers),
            'product': random.choice(products),
            'quantity': random.randint(1, 5),
            'price': round(random.uniform(50, 1500), 2),
            'order_date': datetime.now() - timedelta(days=random.randint(0, 365)),
            'status': random.choice(['completed', 'pending', 'cancelled']),
            'shipping_address': {
                'street': f'{random.randint(100, 999)} Main St',
                'city': random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston']),
                'state': random.choice(['NY', 'CA', 'IL', 'TX']),
                'zip_code': f'{random.randint(10000, 99999)}'
            },
            'metadata': {
                'source': 'web',
                'campaign_id': f'CAMP-{random.randint(1, 10)}'
            }
        }
        sample_orders.append(order)
    
    # Insert data
    collection.insert_many(sample_orders)
    print(f"Inserted {len(sample_orders)} sample orders")
    
    # Create indexes for better performance
    collection.create_index([('order_date', 1)])
    collection.create_index([('customer_email', 1)])
    
    client.close()

if __name__ == "__main__":
    create_sample_data()