import datetime
from google.cloud import storage
from io import BytesIO

from HashHelper import HashHelper

class StorageService(object):
    """description of class"""

    def __init__(self, target_date):
        self.target_date = target_date = target_date.strftime('%Y_%m_%d') if not isinstance(target_date, str) else target_date
        current_year = datetime.datetime.now().year
        self.storage_client = storage.Client()
        self.bucket_name = f"netcoupe-igc-{current_year}"

    
    def GetFileListForDay(self):
        '''
        Get the list of files inside a GCP storage bucket
        '''

        target_date_prefix = self.target_date + "/"
        blobs = self.storage_client.list_blobs(self.bucket_name, prefix = target_date_prefix)

        filenames = []
        for b in blobs:
            filenames.append(b.name)
               
        filenames.sort()
        return filenames


    def GetFileListHashForDay(self):
        '''
        Get a Hash representing the file list for a given day.
        If the hash changes, it means that one or several files were added or deleted
        '''
        sortedList = self.GetFileListForDay()
        hash = HashHelper.ComputeHashForList(sortedList)

        return hash, sortedList


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
