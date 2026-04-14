import boto3
import os
import pandas as pd   #  add kiya

def check_create_bucket(name):  
    s3 = boto3.client('s3')  

    buckets = s3.list_buckets()  

    for bucket in buckets['Buckets']:
        if bucket['Name'] == name:
            print("Bucket Exists")
            return
     
    s3.create_bucket(
        Bucket=name,   
        CreateBucketConfiguration={
            'LocationConstraint': 'ap-south-1'
        }
    )
    print("Bucket Created")


def upload_files(bucket_name): # creating function   

    s3 = boto3.client('s3')   # creating client

    files = os.listdir("downloaded_files")   # download foldeer ma jitni bhi file ha unki list banti ha

    for file in files:  # 

        if file.endswith(".csv"):   #  sirf CSV file process hongi

            file_path = "downloaded_files/" + file  # file path 

            try:
                #  data se year/month nikala
                df = pd.read_csv(file_path, nrows=1)  # nrows mean fest row padega

                year = str(df['Year'][0])   # extract year  columns
                month = str(df['Month'][0]).zfill(2)  # extract month  columns  zfill(2) = > month ko 2 digit bana dega

            except:
                print("Error reading:", file)  # agar file read nahi hui to print error reading
                continue  # and loop ko continue kar dega

            key = year + "/" + month + "/" + file  # foldder structure bana dega

            s3.upload_file(file_path, bucket_name, key) # uploading file 

            print("File uploaded:", file, "-", key)

# input liya
bucket_name = input("Enter bucket name: ").lower()

check_create_bucket(bucket_name)

upload_files(bucket_name)
