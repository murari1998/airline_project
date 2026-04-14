import boto3
# creating unction
def list_all_buckets():  # 
    # list of all buckrt in aws
    s3 = boto3.client("s3")  # s3 se connect karna

    response = s3.list_buckets() #ye aws se request karta hai aur response deta hai

    print(" available buckets:") #output me heading show karega

    for bucket in response["Buckets"]:  #har ek bucket ko access kar rahe hain
        print(f" {bucket['Name']}")  #har bucket ka naam print ho jayega


# Call function
list_all_buckets()