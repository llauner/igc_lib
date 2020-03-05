from datetime import date, datetime
import pytz
import json
import zipfile
import igc_lib
import igc2geojson

from FtpHelper import *
from RunMetadata import RunMetadata

class CumulativeTrackBuilder:

    FTP_TRACKS_ROOT_DIRECTORY = "tracemap/track"

    def __init__(self, ftpClientIgc, ftpClientOut, targetYear, isDryRun=False):
        self.ftpClientIgc = ftpClientIgc
        self.ftpClientOut = ftpClientOut
        self.targetYear = targetYear
        self.isDryRun = isDryRun
  
        self.flightsCount = None
        self.jsonMetadata = None
        
        # Compute targetDate
        self.targetDate = date(targetYear,1,1)     # Start on January 1st
        self.relDaysLookup = 365                   # All year
      
    
    def run(self, allFiles):
        # Get current time in the right time-zone
        tz = pytz.timezone('Europe/Paris')
        scriptStartTime = datetime.now(tz)

        self.flightsCount = len(allFiles)

        flights = []

        # ########## HACK !!!!
        # Take only a few flights during dev...
        del allFiles[50 : len(allFiles)]

        # --- Process files to get flights
        if allFiles:
            for i,filename in enumerate(allFiles):
                zip = FtpHelper.get_file_from_ftp(self.ftpClientIgc, filename)

                with zipfile.ZipFile(zip) as zip_file:
                    flight = igc_lib.Flight.create_from_zipfile(zip_file)
                    if flight.date_timestamp:
                        flight_date = datetime.fromtimestamp(flight.date_timestamp).date()
               
                if flight.valid and flight_date:
                    flights.append(flight)
                   
                    print("{}/{} :{} -> {}".format(i+1,self.flightsCount, filename, zip_file.filelist[0].filename))
                else:
                    print("{}/{} :{} -> {} \t Discarded ! valid={}".format(i+1,self.flightsCount, filename, zip_file.filelist[0].filename, flight.valid))

            # Transform flights into geojson
            featureCollection = igc2geojson.get_geojson_track_collection_full(flights, False)

            # Build metadata
            scriptEndTime = datetime.now(tz)
            metadata = RunMetadata(self.targetDate, scriptStartTime, scriptEndTime, self.flightsCount, 0)

            # Dump
            self._dumpToFtp(featureCollection, metadata)


    def _dumpToFtp(self, featureCollection, metadata):
        # Tracks
        tracksFilename = f"{self.targetYear}-tracks.geojson"
        jsonTracks = str(featureCollection)

        # Meta-data
        metadataFilename = f"{self.targetYear}-tracks-metadata.json"
        self.jsonMetadata = metadata.toJSON()
        
        # Dump to FTP
        if not self.isDryRun:
           igc2geojson.dump_string_to_ftp(self.ftpClientOut, CumulativeTrackBuilder.FTP_TRACKS_ROOT_DIRECTORY, tracksFilename, jsonTracks)
        else:
            geojsonFileContent = str(featureCollection)
            print(featureCollection)
            print(jsonMetadata)


    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)
   