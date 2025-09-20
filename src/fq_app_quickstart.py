from pymongo import MongoClient

uri = "mongodb+srv://ls_db_user:s47tBnWikllAoe3k@democluster0.j777gry.mongodb.net/?retryWrites=true&w=majority&appName=DemoCluster0"
client = MongoClient(uri)

try:
    database = client.get_database("FQ_App")
    accounts = database.get_collection("accounts")

    # Query for a an account from accounts
    query = { "account_id": "1000" }
    account = accounts.find_one(query)

    print(account)

    client.close()

except Exception as e:
    raise Exception("Unable to find the document due to the following error: ", e)
