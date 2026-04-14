import boto3
# creating a function
def check_and_create_bucket():
    s3 = boto3.client("s3", region_name="ap-south-1")   # create s3 client

    bucket_name = input("Enter bucket name: ")   # take input from user

    buckets = s3.list_buckets()["Buckets"]   # ya aws ke sabhi bucket ki list ki list return karta ha
  
    exists = any(b["Name"] == bucket_name for b in buckets)  # check buket is exist or not 
                                                             # for b in bucket -> har bucket check karna  #b["Name"] == bucket_name => name check karna
                                                             # any() = > agar koi bhi mila true
    if exists:
        print(" already exists ")    # agar bucket ha to print already exist
    else:
        s3.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={
                "LocationConstraint": "ap-south-1"    
            }
        )     # agar bucket nahi ha to create a bucket 
        print("Bucket created ") # print massage


check_and_create_bucket()  # calling function