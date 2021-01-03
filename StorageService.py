import datetime
from google.cloud import storage
from io import BytesIO

from HashHelper import HashHelper

class StorageService(object):
    """description of class"""

    Trace_aggregator_bucket_name = "tracemap-trace-aggregator"     # Tracemap aggregator bucket name
    Trace_aggregator_backlog_folder_name = "backlog" 
    Trace_aggregator_alternative_source_bucket_name = "netcoupe-igc-source"


    def __init__(self, target_date, bucket_name=None):
        if target_date:
            self.target_date = target_date = target_date.strftime('%Y_%m_%d') if not isinstance(target_date, str) else target_date
        else:
            self.target_date = None
        current_year = datetime.datetime.now().year
        self.storage_client = storage.Client()
        if bucket_name is None:
            self.bucket_name = f"netcoupe-igc-{current_year}"
        else:
            self.bucket_name = bucket_name

    
    def GetFileListForDay(self):
        '''
        Get the list of files inside a GCP storage bucket
        '''

        target_date_prefix = self.target_date + "/"
        blobs = self.storage_client.list_blobs(self.bucket_name, prefix = target_date_prefix)

        filenames = []
        for b in blobs:
            if b.name.endswith(".igc"):
                filenames.append(b.name)
               
        filenames.sort()
        return filenames

    def GetFileListForAlternativeSource(self, folder_name):
        '''
        Get the list of files inside a GCP storage bucket: Alternative source bucket = netcoupe-igc-source
        '''
        blobs = self.storage_client.list_blobs(self.bucket_name, prefix = folder_name)
        filenames = []
        for b in blobs:
            if b.name.endswith(".igc"):
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


    def GetFileAsStringFromBucket(self, bucket_name, filename):
        '''
        Get a file from the GCP storage bucket
        '''
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(filename)
        f = BytesIO()
        blob.download_to_file(f)
        f.seek(0)

        return f

    def GetFileAsString(self, filename):
        return self.GetFileAsStringFromBucket(self.bucket_name, filename)

    def GetFileFullpathFromName(self, filename, filename_last_year = None):
        '''
        Get the file fullpath for a given filename
        Will try in bucket from previous year if we can't find it in the current year
        '''
        blobs = list(self.storage_client.list_blobs(self.bucket_name, fields='items(name),nextPageToken'))
        filesFound = [x for x in blobs if filename in x.name]

        if (len(filesFound)>0):
            return self.bucket_name, filesFound[0].name
        # Could not find it for this year
        elif filename_last_year is not None:
            last_year = datetime.datetime.now().year - 1
            last_year_bucket_name = f"netcoupe-igc-{last_year}"
            blobs = list(self.storage_client.list_blobs(last_year_bucket_name, fields='items(name),nextPageToken'))
            filesFound = [x for x in blobs if filename_last_year in x.name]
            if (len(filesFound)>0):
                return last_year_bucket_name, filesFound[0].name
        return None, None

    def UploadFileToBucket(self, file, bucket_name, folder_name, file_name):
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{folder_name}/{file_name}")
        blob.upload_from_file(file, rewind=True)

    def UploadStringToBucket(self, string_to_upload, bucket_name, folder_name, file_name):
        bucket = self.storage_client.bucket(bucket_name)
        blob = bucket.blob(f"{folder_name}/{file_name}")
        blob.upload_from_string(string_to_upload)

    
    def DeleteFilesFromBucket(self, file_list, bucket_name):
        bucket = self.storage_client.bucket(bucket_name)
        bucket.delete_blobs(file_list)


