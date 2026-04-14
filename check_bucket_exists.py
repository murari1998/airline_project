import boto3
# creating function
def check_bucket_exists(bucket_name):  # jis bucket ko check karna ha uska naam
    s3 = boto3.client("s3")  # create s3 client

    #  
    buckets = s3.list_buckets()["Buckets"]  #  ya aws ke sabhi bucket ki list ki list return karta ha

    for bucket in buckets:  # har bucket ko check karna
        if bucket["Name"] == bucket_name:   # agar bucket ka naam match hua to true return karega
            return True

    return False    # otherwise false return karega


# User se input lena
user_bucket = input("Enter bucket name: ")

if check_bucket_exists(user_bucket):  # agar bucket alrady hui to print already exist
    print(f"already  exists ")
else:
    print(f"bucket not exist ")   # agar bucket nahi hui to print buccket not exist