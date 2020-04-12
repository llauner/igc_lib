import json

class RunStatistics(object):
    def __init__(self):
        self.flightsPerDate = {}	# TimeSeries dictionary
        
    def addTimeSeries(self, date):
        if self.flightsPerDate.get(date) is not None:
            self.flightsPerDate[date] +=1
        else:
            self.flightsPerDate.update({date:1})
            
    def __getAsTimeSeriesArray(self):
        dict = {}
        list = [TimeSeries(k, v) for k, v in self.flightsPerDate.items()]
        list.sort(key=lambda x: x.date)
        return list
    
    def toJson(self):
        statistics = self.__getAsTimeSeriesArray()
        jsonStatistics = json.dumps(statistics, default=lambda o: o.__dict__, sort_keys=True)
        return jsonStatistics

class TimeSeries(object):
    def __init__(self, date, value):
        self.date = date
        self.value = value