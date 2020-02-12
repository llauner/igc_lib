from datetime import datetime, date, time, timedelta
import pytz
import json

class RunMetadata:
    def __init__(self, target_date, script_start_time, script_end_time, flights_count, thermals_count):
        self.targetDate = str(target_date)
        self.startDate = str(script_start_time)
        self.endDate =  str(script_end_time)
        self.duration = str(script_end_time - script_start_time)
        self.flightsCount = int(flights_count)
        self.thermalsCount = int(thermals_count)


    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)
   