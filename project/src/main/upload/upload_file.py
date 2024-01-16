import traceback
from project.src.main.utility.s3_client import s3clientprovider
from project.src.main.utility.config import *
from project.src.main.utility.logging_config import *
import datetime

class uploadtoS3:
    def __init__(self,s3_client):
        self.s3_client = s3_client
        print(s3_client)

    def upload_to_s3(self,s3_directory,s3_bucket,local_file_path):
        current_epoch = int(datetime.datetime.now().timestamp()) * 1000
        s3_prefix = f"{s3_directory}/{current_epoch}/"
        try:
            for root, dirs, files in os.walk(local_file_path):
                for file in files:
                       local_file_path = os.path.join(root, file)
                       s3_key = f"{s3_prefix}/{file}"
                       self.s3_client.upload_file(local_file_path, s3_bucket, s3_key)
            return s3_prefix
        except Exception as e:
            logger.error(f"Error uploading file : {str(e)}")
            traceback_message = traceback.format_exc()
            print(traceback_message)
            raise e

"""
s3 = s3clientprovider(access_key_id,secret_access_key)
#print(s3.get_client())
obj = uploadtoS3(s3.get_client())
s3_prefix = obj.upload_to_s3(s3_directory,s3_bucket,local_file_path)
"""


