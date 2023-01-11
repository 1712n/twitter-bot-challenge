import os

mongodb_user = os.environ["MONGODB_USER"]
mongodb_password = os.environ["MONGODB_PASSWORD"]
mongodb_address = os.environ["MONGODB_ADDRESS"]

print(mongodb_address)
