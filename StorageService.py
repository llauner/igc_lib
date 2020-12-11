from google.cloud import storage
from io import BytesIO

class StorageService(object):
    """description of class"""

    def __init__(self):
        self.storage_client = storage.Client()
        self.bucket_name = "netcoupe-igc-2020"

    
    def GetFileList(self, target_date):
        '''
        Get the list of files inside a GCP storage bucket
        '''
        target_date_prefix = target_date.strftime("%Y_%m_%d") + "/"
        blobs = self.storage_client.list_blobs(self.bucket_name, prefix = target_date_prefix)

        filenames = []
        for b in blobs:
            filenames.append(b.name)
               
        return filenames


    def GetFileAsString(self, filename):
        '''
        Get a file from the GCP storage bucket
        '''
        bucket = self.storage_client.bucket(self.bucket_name)
        blob = bucket.blob(filename)
        f = BytesIO()
        blob.download_to_file(f)
        f.seek(0)

        return f
