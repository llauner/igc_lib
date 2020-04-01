from datetime import date, datetime
import pytz
import json
import zipfile
import igc_lib
import igc2geojson
import numpy as np
from PIL import Image, ImageDraw
import pyproj
import math

import io

import matplotlib.pyplot as plt
import matplotlib as mpl

from FtpHelper import *
from RunMetadata import RunMetadata

MAP_WIDTH = 1000
MAP_HEIGHT = 800

P = pyproj.Proj(proj='utm', zone=31, ellps='WGS84', preserve_units=True)
    
def LatLon_To_XY(Lat,Lon):
    return P(Lat,Lon)  

class CumulativeTrackBuilder:

    FTP_TRACKS_ROOT_DIRECTORY = "tracemap/track"
    
    FRANCE_BOUNDING_BOX = [(-6.566734,51.722775), (10.645924,51.726922), (10.625153,42.342052), (-6.673679,42.318955)]

    def __init__(self, metaData, ftpClientIgc, ftpClientOut, targetYear, useLocalDirectory=False, isDryRun=False):
        self.metaData = metaData
        self.ftpClientIgc = ftpClientIgc
        self.ftpClientOut = ftpClientOut
        self.targetYear = targetYear
        self.useLocalDirectory = useLocalDirectory
        self.isDryRun = isDryRun

        self.flightsCount = None
        self.jsonMetadata = None
        
        # Compute targetDate
        self.targetDate = date(targetYear,1,1)     # Start on January 1st
        self.relDaysLookup = 365                   # All year

    
    def run(self, allFiles):
        self.flightsCount = len(allFiles)

        flights = []

        # ########## HACK !!!!
        # Take only a few flights during dev...
        #del allFiles[10 : len(allFiles)]

        # --- Process files to get flights
        if allFiles:
            for i,filename in enumerate(allFiles):
                computedFileName = None
                flight = None
                # ----- File from FTP -----
                if not self.useLocalDirectory: 
                    zip = FtpHelper.get_file_from_ftp(self.ftpClientIgc, filename)

                    with zipfile.ZipFile(zip) as zip_file:
                        computedFileName = zip_file.filelist[0].filename
                        flight = igc_lib.Flight.create_from_zipfile(zip_file)
                        
                # ----- File from local directory -----
                else:
                    computedFileName = "OK"
                    flight = igc_lib.Flight.create_from_file(filename)
                        
                if flight.date_timestamp:
                        flight_date = datetime.fromtimestamp(flight.date_timestamp).date()
                if flight.valid and flight_date:
                    flights.append(flight)
                    print("{}/{} :{} -> {}".format(i+1,self.flightsCount, filename, computedFileName))
                else:
                    print("{}/{} :{} -> {} \t Discarded ! valid={}".format(i+1,self.flightsCount, filename, computedFileName, flight.valid))

            self.dumpFlightsToImage(flights)
        
        
    def dumpFlightsToImage(self, listFlights):
        from shapely.geometry import Point, LineString, Polygon
        import geopandas as gpd
        from geopandas import GeoDataFrame, GeoSeries
        import pandas as pd
        
        import mplleaflet
        
        lons = np.vectorize(lambda f: f.lon)
        lats = np.vectorize(lambda f: f.lat)
        
        
        franceBoundingBox = Polygon(self.FRANCE_BOUNDING_BOX)
        
        axes = None
        # --- Setup plot ---
        mpl.rcParams['savefig.pad_inches'] = 0
        figsize = None
        fig = plt.figure(figsize=figsize)
        ax = plt.axes([0,0,1,1], frameon=False)
        ax.get_xaxis().set_visible(False)
        ax.get_yaxis().set_visible(False)
        plt.autoscale(tight=True)
        
        # --- Browse and plot files ---
        for i,flight in enumerate(listFlights):
            longitudes = lons(flight.fixes)
            latitudes = lats(flight.fixes)
            flightPoints = list(zip(longitudes.tolist(), latitudes.tolist()))
        
            flightLineString = LineString(flightPoints)
            geoSeries = GeoSeries(flightLineString)
            
            # Check that the flight is inside the France polygon
            isInsindeFrance = franceBoundingBox.contains(flightLineString)
            
            if isInsindeFrance:
                if i==0:
                    axes = geoSeries.plot(ax=ax, figsize=(20,20), linewidth=0.5)
                else:
                    axes = geoSeries.plot(ax=axes, linewidth=0.5)
            
            else:
                print("Discarded: not inside France !")
        
        self.metaData.boundingBoxUpperLeft = [ax.dataLim.extents[1], ax.dataLim.extents[0]]
        self.metaData.boundingBoxLowerRight = [ax.dataLim.extents[3], ax.dataLim.extents[2]]
        
        print(self.metaData.boundingBoxUpperLeft)
        print(self.metaData.boundingBoxLowerRight)
        
        self._dumpToLocalFiles()
        
        print("ok")
        

    def _dumpToLocalFiles(self):
        # Dump to HTML (results in a very large file)
        #mplleaflet.show(path='/Users/llauner/Downloads/latest-tracks.html')

        # Dump image
        plt.savefig('/Users/llauner/Downloads/latest-tracks.png', format='png', dpi=200, transparent=True, bbox_inches='tight', pad_inches=0)
        plt.close()
        
        # Build metadata
        tz = pytz.timezone('Europe/Paris')
        self.metaData.setEndTime(datetime.now(tz))
        
        # Dump MetaData
        self.jsonMetadata = self.metaData.toJSON()
        with open('/Users/llauner/Downloads/latest-tracks-metadata.json', 'w') as jsonOut:
            jsonOut.write(self.jsonMetadata)


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