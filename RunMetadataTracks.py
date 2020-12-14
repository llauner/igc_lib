from datetime import datetime, date, time, timedelta
import pytz
import json
from flask.json import JSONEncoder


class RunMetadataTracks(object):
    def __init__(self, target_date):
        # Get current time in the right time-zone
        tz = pytz.timezone('Europe/Paris')
        self.targetDate = str(target_date)

        self.script_start_time = datetime.now(tz)
        self.script_end_time = None
        
        self.flightsCount = 0
        self.processedFlightsCount = 0


    def toJSON(self):
        self.script_start_time = str(self.script_start_time)
        self.script_end_time = str(self.script_end_time)
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)

    def to_json(self):
        self.script_start_time = str(self.script_start_time)
        self.script_end_time = str(self.script_end_time)
        return self.__dict__
