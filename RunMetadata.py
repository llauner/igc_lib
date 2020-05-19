from datetime import datetime, date, time, timedelta
import pytz
import json
from flask.json import JSONEncoder


class RunMetadata(object):
    def init(self, target_date, script_start_time, script_end_time, flights_count, thermals_count):    
        # Get current time in the right time-zone
        tz = pytz.timezone('Europe/Paris')
        
        self.targetDate = str(target_date)
        script_start_time = datetime.now(tz)
        self.script_start_time = script_start_time
        self.startDate = str(script_start_time)
        self.endDate =  str(script_end_time)
        if not script_end_time is None:
            self.duration = str(script_end_time - script_start_time)
        self.flightsCount = int(flights_count)
        self.thermalsCount = int(thermals_count)
        self.boundingBoxUpperLeft = []
        self.boundingBoxLowerRight = []
        self.processedFlightsCount = 0
        
    def __init__(self):
        # Get current time in the right time-zone
        tz = pytz.timezone('Europe/Paris')
        script_start_time = datetime.now(tz)
        target_date = date(script_start_time.year, script_start_time.month, script_start_time.day)
    
        self.targetDate = str(target_date)
        self.script_start_time = script_start_time
        self.startDate = str(script_start_time)
        
        self.endDate =  None
        self.script_end_time = None   
        self.flightsCount = 0
        self.processedFlightsCount = 0
        self.thermalsCount = 0
        self.boundingBoxUpperLeft = []
        self.boundingBoxLowerRight = []
    

    def setEndTime(self, script_end_time):
        if self.endDate is None:
            self.endDate =  str(script_end_time)
            self.duration = str(script_end_time - self.script_start_time)
        
    def toJSON(self):
        self.script_start_time = str(self.script_start_time)
        self.script_end_time = str(self.endDate)
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)
    
    def to_json(self):
        self.script_start_time = str(self.script_start_time)
        self.script_end_time = str(self.endDate)
        return self.__dict__

    
