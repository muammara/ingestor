# from minio import Minio
# from minio.error import S3Error
# from minio.commonconfig import Tags
import os
import mysql.connector
import argparse
from mysql.connector import errorcode
import hashlib
import sys
import boto3
from botocore.client import Config
import traceback




def main():

    ############### Reading command line arguments ###############
    # first we parse the command line arguments provided, these are:
    # 1. A directory name which will contain images
    # 2. a label for all the images in that directory: is_fake = True or False

    parser = argparse.ArgumentParser()
    parser.add_argument('images_path', type=str,
                    help='The absolute path to the directory containing the images - required')

    parser.add_argument('--fake', action='store_true',
                    help='The fake flag. When set, is_fake will be set to true')

    args = parser.parse_args()
    path = args.images_path
    is_fake = args.fake

    if not os.path.exists(path):
        print(f"Path {path} does not exist. Please ensure it is an absolute path")
        return


    ############### Connecting to S3 ###############

    print(f"Attempting to connect to S3 at {s3_address}")

    client = None
    try:
        client = boto3.client('s3',
          endpoint_url=s3_address,
          aws_access_key_id=s3_access_key,
          aws_secret_access_key=s3_secret_key,
          config=boto3.session.Config(signature_version='s3v4')
        )
    except Exception as e:
        print("An unexpected exception occured", e)
        return

    # make a bucket called deepfakes if it does not already exist

    try:
        response = client.create_bucket(
            Bucket=bucket_name,
        )
        print(f"Bucket '{bucket_name}' was created")
    except (client.exceptions.BucketAlreadyOwnedByYou, client.exceptions.BucketAlreadyExists) as e:
        print("Bucket '{bucket_name}' already exists")
    except Exception as e:
        traceback.print_exc()
        print("An unexpected exception occured", e)
        return




    # tags = Tags(for_object=True)
    # tags["is_fake"] = str(is_fake)


    ############### Connecting to Docker MySQL instance ###############
    db_connection = None
    print(f"Attempting to connect to MySQL instance at {sql_host}")
    try:
        db_connection = mysql.connector.connect(
                            user=sql_username,
                            password=sql_password,
                            host=sql_host,
                            database='images',
                            port=sql_port)

    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Incorrect username or password for DB connection")
            return
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
            return
        else:
            print(err)
            return
    except Exception as e:
        print("An unexpected exception occured", e)
        return

    print("Successfully connected to MySQL. Database images was found")


    ############### Checking if images table exists ###############

    print("Checking for images table...")
    query = "SHOW TABLES LIKE 'images'"
    cursor = db_connection.cursor()
    cursor.execute(query)

    if not cursor.fetchone():
        try:
            print("Table images was not found... will create table")
            cursor.reset()
            query = "CREATE TABLE `images` (\
                `id` INT NOT NULL AUTO_INCREMENT,\
                `hash` CHAR(64) NOT NULL,\
                `is_fake` VARCHAR(45) NOT NULL,\
                PRIMARY KEY (`id`),\
                UNIQUE INDEX `hash_UNIQUE` (`hash` ASC) );"

            cursor.execute(query)
            db_connection.commit()
            print("Successfully created table images")
            cursor.reset()

        except (mysql.connector.Error, mysql.connector.Warning) as e:
            print("Could not create table ... aborting")
            print(e)
            return
        except Exception as e:
            print("An unexpected exception occured", e)
            return

    else:
        cursor.reset()
        print("Table images was found")


    ############### Performing insert operation ###############
    print("Attempting to insert data...")

    # first we obtain all the hashes from the DB
    query = "SELECT hash FROM images;"
    cursor.execute(query)
    all_hashes = [h[0] for h in cursor]

    # tags = Tags(for_object=True)
    # tags["is_fake"] = str(is_fake)
    metadata = {
        'Content-type': 'image',
    };

    for root, dirs, files in os.walk(path, topdown=True):
        for file in files:
            if file.lower().endswith(('.png', '.jpg', '.jpeg', '.tiff', '.bmp', '.gif')):
                cursor.reset()
                file_path = os.path.join(path, file)
                image_hash = generateHash(file_path)

                if not (image_hash in all_hashes):
                    # we perfrom the insetion into minio first
                    file_name = image_hash + "." + file.split(".")[-1]

                    try:
                        result = client.upload_file(file_path, bucket_name, file_name)

                    except Exception as e:
                        print("An unexpected exception occured", e)
                        return

                    print(f"Added image {file} to minio bucket {bucket_name} with is_fake set to {is_fake}")

                    # now to add the images to the mysql database

                    query = ("INSERT INTO images "
                            "(hash, is_fake) "
                            "VALUES (%s, %s)")
                    query_data = (image_hash, is_fake)

                    try:
                        cursor.execute(query, query_data)
                        print(f"Added image {file} to MySQL Database")
                    except (mysql.connector.Error, mysql.connector.Warning) as e:
                        print("Failed to insert into MySQL")
                        print(e)


                else:
                    print("Image already exists ... skipping")

    db_connection.commit()
    db_connection.close()

def generateHash(file_path):

    BUF_SIZE = 65536

    sha2 = hashlib.sha256()

    with open(file_path, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha2.update(data)

    return sha2.hexdigest()

def formUrl(file_name):
    return f"{s3_policy}://{s3_address}/{bucket_name}/{file_name}"



if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print("error occurred.", e)
