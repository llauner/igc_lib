from FirestoreService import FirestoreService


class HeatmapFirestoreService(FirestoreService):
    """description of class"""

   
    @property
    def FirestoreCollectionName(self):
        return 'heatmapProgress'
    
    @property
    def FirestoreDocumentName(self):
        return '{0}_heatmapBuilder'


 

