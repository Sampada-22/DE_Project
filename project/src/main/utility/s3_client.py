from project.src.main.utility.config import *
import boto3


class s3clientprovider:
    def __init__(self,access_key_id,secret_access_key):
        self.aws_access_key = access_key_id
        self.aws_secret_key = secret_access_key
        self.session = boto3.Session(
            aws_access_key_id=self.aws_access_key,
            aws_secret_access_key=self.aws_secret_key
        )
        self.s3_client = self.session.client('s3')
        self.s3 = boto3.resource('s3')

    def get_client(self):
        return self.s3_client,self.s3


#s3 = s3clientprovider(access_key_id,secret_access_key)
#print(s3.get_client())

