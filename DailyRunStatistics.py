import json

class DailyRunStatistics(object):
    def __init__(self):
        self.flightsCount = 0
    
    def toJson(self):
        statistics = self.__getAsTimeSeriesArray()
        jsonStatistics = json.dumps(statistics, default=lambda o: o.__dict__, sort_keys=True)
        return jsonStatistics

