from project.src.main.utility.s3_client import s3clientprovider
from project.src.main.utility.config import *
from project.src.main.upload.upload_file import *
from project.src.main.utility.logging_config import *
from project.src.main.download.S3_file_download import *


if __name__ == "__main__":
    s3obj = s3clientprovider(access_key_id, secret_access_key)
    s3_client, s3 = s3obj.get_client()
    print(s3_client,s3)
    print("S3 client generated")
    #obj = uploadtoS3(s3_client)
    #object_key = obj.upload_to_s3(s3_directory, s3_bucket, local_file_path)
    #print("Files are successfully uploaded to folder %s" %(object_key))
    #object_key = "new_folder/1703260586000/"
    #obj1 = s3download(s3_client,s3)
    #obj1.download_file_from_s3(s3_bucket,object_key, local_download_folder_path)
    #print(f"Files downloaded successfully")
