import os
import logging
import pickle
import tempfile
import warnings
from pathlib import Path

import pandas as pd

import boto3
from dotenv import load_dotenv
from botocore.client import Config
from botocore.exceptions import ClientError
from urllib3.exceptions import InsecureRequestWarning
from .utils import str_to_bool


load_dotenv()
logger = logging.getLogger(__name__)
warnings.simplefilter('ignore', InsecureRequestWarning)


class S3(object):

    def __init__(self):
        self.bucket_name = os.environ["S3_BUCKET_NAME"]
        self.client = boto3.client(
            's3',
            endpoint_url=os.environ["S3_URL"],
            aws_access_key_id=os.environ["S3_USER"],
            aws_secret_access_key=os.environ["S3_PASSWORD"],
            config=Config(signature_version='s3v4'),
            verify=str_to_bool(os.environ["S3_SSL_VERIFY"])
        )
        self.download_path = os.environ["S3_DOWNLOAD_PATH"]

    def exists(self, path):
        return os.path.exists(path)

    def download(self, file_name, target_path=os.environ["S3_DOWNLOAD_PATH"]):
        target_file = os.path.join(target_path, file_name)
        if self.exists(target_file):
            logger.info(f"{file_name} already exists at {target_file}. Skipping download.")
            return True
        file_dir = os.path.join(self.download_path, os.path.dirname(file_name))
        if not os.path.exists(file_dir):
            logger.info(f"Creating folder {file_dir}")
            Path(file_dir).mkdir(parents=True, exist_ok=True)
        try:
            self.client.download_file(self.bucket_name, file_name, target_file)
            return True
        except Exception as e:
            logger.error(e)
            return False

    def upload(self, file_path, target_name=None):
        if target_name is None:
            target_name = os.path.basename(file_path)
        try:
            self.client.upload_file(file_path, self.bucket_name, target_name)
        except ClientError as e:
            logger.error(e)
            return False
        return True

    def upload_dataframe(self, df, file_name, format):
        assert isinstance(df, pd.DataFrame)
        tmp_path = tempfile.mkstemp()[1]
        if format == "parquet":
            df.to_parquet(tmp_path)
        elif format == "excel":
            tmp_path += ".xlsx"
            df.to_excel(tmp_path, index=None)
        else:
            raise NotImplementedError(f"format {format} is not implemented.")
        self.upload(tmp_path, file_name)

    def upload_pickle(self, obj, file_name):
        tmp_path = tempfile.mkstemp()[1]
        with open(tmp_path, "wb") as fp:
            pickle.dump(obj, fp, protocol=pickle.HIGHEST_PROTOCOL)
        self.upload(tmp_path, file_name)

    def download_dir(self, dir_name):
        result = True
        for target_file in self.list_dir_files(dir_name):
            result = result and self.download(target_file)
        return result

    def list_dir_files(self, dir_name):
        ret = []
        try:
            ret = [
                el["Key"]
                for el in self.client.list_objects(
                    Bucket=self.bucket_name, Prefix=dir_name
                )["Contents"]
            ]
        except Exception as e:
            logger.error("unable to list {} dir files on s3.".format(dir_name))
            logger.exception(e)
        return ret

