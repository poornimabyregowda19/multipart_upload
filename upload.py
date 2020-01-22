import os
import sys
import threading

import boto3
from boto3.s3.transfer import TransferConfig


s3 = boto3.resource('s3')

def multi_part_upload_with_s3(BUCKET_NAME, file_name):
    # Multipart upload
    config = TransferConfig(multipart_threshold=1024 * 25, max_concurrency=10,
                            multipart_chunksize=1024 * 25, use_threads=True)
    file_path = os.getcwd() + '/' + file_name

    if not os.path.isfile(file_path):
        raise Exception("file doesnot exist")

    key_path = 'test/' + file_name
    s3.meta.client.upload_file(file_path, BUCKET_NAME, key_path,
                               ExtraArgs={'ACL': 'bucket-owner-full-control', 'ContentType': 'text/pdf'},
                               Config=config,
                               Callback=ProgressPercentage(file_path)
                            )

class ProgressPercentage(object):
    def __init__(self, filename):
        self._filename = filename
        self._size = float(os.path.getsize(filename))
        self._seen_so_far = 0
        self._lock = threading.Lock()

    def __call__(self, bytes_amount):
        # To simplify we'll assume this is hooked up
        # to a single filename.
        with self._lock:
            self._seen_so_far += bytes_amount
            percentage = (self._seen_so_far / self._size) * 100
            sys.stdout.write(
                "\r%s  %s / %s  (%.2f%%)" % (
                    self._filename, self._seen_so_far, self._size,
                    percentage))
            sys.stdout.flush()


if __name__ == "__main__":
    file_name = "../multi_upload/databases.pdf"
    BUCKET_NAME = "testfederationtoken"
    multi_part_upload_with_s3(BUCKET_NAME,file_name)
