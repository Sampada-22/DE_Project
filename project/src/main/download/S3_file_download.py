import boto3
from project.src.main.utility.s3_client import s3clientprovider
from project.src.main.utility.config import *
from project.src.main.upload.upload_file import *
from project.src.main.utility.logging_config import *
import datetime

class s3download:
    def __init__(self,s3_client,s3):
        self.s3_client = s3_client
        self.s3 = s3

    def download_file_from_s3(self,s3_bucket, object_key, local_download_folder_path):
        list_files = []

        folder_path = f"{s3_bucket}/{object_key}"
        response = self.s3_client.list_objects(Bucket = s3_bucket, Prefix =object_key )
        for content in response.get('Contents', []):
            list_files.append(content['Key'])

        try:
            for key in list_files:
                file_name = os.path.basename(key)
                #print(file_name)
                local_file_path = os.path.join(local_download_folder_path,file_name)
                self.s3_client.download_file(s3_bucket,key,local_file_path)
            print(f"File downloaded successfully to: {local_download_folder_path}")
        except Exception as e:
            print(f"Error downloading file: {e}")


"""
s3 = s3clientprovider(access_key_id,secret_access_key)
#print(s3.get_client())
obj = uploadtoS3(s3.get_client())
object_key = obj.upload_to_s3(s3_directory,s3_bucket,local_file_path)
#download file
obj1 = s3download(s3.get_client())
obj1.download_file_from_s3(s3_bucket, object_key, local_download_folder_path)
"""