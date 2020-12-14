import datetime
from google.cloud import firestore

from HashHelper import HashHelper


class FirestoreService(object):
    """description of class"""

    FirestoreCollectionName = 'tracemapProgress'
    FirestoreDocumentName =  '{0}_dailyCumulativeTrackBuilder'
    FirestoreFieldName = 'processedDays'

    def __init__(self, target_date):
        current_year = datetime.datetime.now().year
        self.db = firestore.Client()
        self.target_date = target_date = target_date.strftime('%Y_%m_%d') if not isinstance(target_date, str) else target_date

        FirestoreService.FirestoreDocumentName =  FirestoreService.FirestoreDocumentName.format(current_year)
        
    def GetProcessedFilesHashForDay(self):
        """Get the hash for the list of processed files from the DB

        Keyword arguments:
        target_date -- the target date
        """
 
        doc_ref = self.db.collection(FirestoreService.FirestoreCollectionName).document(FirestoreService.FirestoreDocumentName)
        snapshot = doc_ref.get().to_dict()
        dict_processed_days = snapshot[FirestoreService.FirestoreFieldName]
        
        return dict_processed_days.get(self.target_date)


    def UpdateProcessedFilesHasForDay(self, file_list):
        """Update the hash for the target date

        Keyword arguments:
        file_list --- the sorted list of processed files
        target_date -- the target date
        """

        doc_ref = self.db.collection(FirestoreService.FirestoreCollectionName).document(FirestoreService.FirestoreDocumentName)
        snapshot = doc_ref.get()

        new_hash = HashHelper.ComputeHashForList(file_list) if file_list else None
        field_update = { f"{FirestoreService.FirestoreFieldName}.{self.target_date}" : new_hash }

        doc_ref.update(field_update)
        print(f"Updated hash for target date: {self.target_date} : {new_hash}")
 

