from datetime import date, datetime
import pytz
import json
import zipfile
import igc_lib
import igc2geojson
import numpy as np

import io

import matplotlib.pyplot as plt
import matplotlib as mpl
from shapely.geometry import Point, LineString, Polygon
import geopandas as gpd
from geopandas import GeoDataFrame, GeoSeries
import pandas as pd
import mplleaflet
from progress.bar import Bar

from FtpHelper import *
from RunMetadata import RunMetadata

class CumulativeTrackBuilder:
    


    # --- Files dans folders ---
    FTP_TRACKS_ROOT_DIRECTORY = "tracemap/tracks/"
    TRACKS_IMAGE_FILE_NAME = "latest-tracks.png"
    TRACKS_METADATA_FILE_NAME = "latest-tracks-metadata.json"
    TRACKS_LOCAL_DUMP_DIRECTORY = "/Users/llauner/Downloads/"
    
    # --- Geo information ---
    FRANCE_BOUNDING_BOX = [(-6.566734,51.722775), (10.645924,51.726922), (10.625153,42.342052), (-6.673679,42.318955)]
    
    # --- Iamge settings ---
    IMAGE_DPI = 1800
    #IMAGE_SIZE=(20,20)
    LINE_WIDTH = 0.05
    
    # --- Dev and Debug ---
    NB_FILES_TO_KEEP = None
    
    def __init__(self, ftpClientIgc, ftpClientOut, targetYear = None, useLocalDirectory=False, isOutToLocalFiles=False):
        self.metaData = RunMetadata()
        self.ftpClientIgc = ftpClientIgc
        self.ftpClientOut = ftpClientOut
        self.targetYear = datetime.now().year if targetYear is None else targetYear
        self.useLocalDirectory = useLocalDirectory
        self.isOutToLocalFiles = isOutToLocalFiles

        self.fileList = []
        self.flightsCount = None
        
        self.progressMessage = None
        
        # Compute targetDate
        self.targetDate = date(self.targetYear,1,1)     # Start on January 1st
        self.relDaysLookup = 365                   # All year
        
        self.franceBoundingBox = Polygon(self.FRANCE_BOUNDING_BOX)
        
        self.isRunningInCloud = not (self.useLocalDirectory and self.isOutToLocalFiles)
        
        # --- Setup plot ---
        mpl.rcParams['savefig.pad_inches'] = 0
        mpl.use('agg') 
        figsize = None
        fig = plt.figure(figsize=figsize)
        self.axes = plt.axes([0,0,1,1], frameon=False)
        self.axes.get_xaxis().set_visible(False)
        self.axes.get_yaxis().set_visible(False)
        plt.autoscale(tight=True)

    def run(self):
        if not self.useLocalDirectory:
            allFiles = FtpHelper.get_file_names_from_ftp(self.ftpClientIgc, self.targetDate, self.relDaysLookup)
        else:
            allFiles = FtpHelper.getFIlenamesFromLocalFolder()
        self.fileList = allFiles
        self._run(self.fileList)
        
        return self.metaData
        
    def _run(self, allFiles):
        self.fileList = allFiles
        self.metaData.flightsCount = len(allFiles)

        # DEv or Debug: Take only a few flights during dev...
        if CumulativeTrackBuilder.NB_FILES_TO_KEEP:
            del allFiles[0 : len(allFiles)-CumulativeTrackBuilder.NB_FILES_TO_KEEP]

        # --- Process files to get flights
        if allFiles:
            progressBar = CumulativeTrackBuilder.SlowBar('Processing', max=self.metaData.flightsCount)
            
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
                    flight = igc_lib.Flight.create_from_file(filename)
                        
                if flight.date_timestamp:
                        flight_date = datetime.fromtimestamp(flight.date_timestamp).date()
                # --- Build progress message and update       
                if flight.valid and flight_date:
                    if computedFileName:
                        progressBar.progressMessage = "{} -> {}".format(filename, computedFileName)
                    else:
                        progressBar.progressMessage = "{}".format(filename)
                else:
                    if computedFileName:
                        progressBar.progressMessage = "{} -> {} \t Discarded ! valid={}".format(filename, computedFileName, flight.valid)
                    else:
                        progressBar.progressMessage = "{} \t Discarded ! valid={}".format(filename, flight.valid)

                if self.isRunningInCloud:           # ProgressBar does not work in gcloud: pring on stdout
                    print(progressBar.getLine())
                else:
                    progressBar.next()  # Update Progress

                # ----- Process flight -----
                self.dumpFlightsToImage(flight)
            
            progressBar.finish()    # End Progress
                
            # --- Save bounding box ---
            self.metaData.boundingBoxUpperLeft = [self.axes.dataLim.extents[1], self.axes.dataLim.extents[0]]
            self.metaData.boundingBoxLowerRight = [self.axes.dataLim.extents[3], self.axes.dataLim.extents[2]]
            
        if self.isOutToLocalFiles:      # Dump to local files
            self._dumpToFiles()    
        else:                           # Dump to FTP
            self._dumpToFtp()
        
    def dumpFlightsToImage(self, flight):
        lons = np.vectorize(lambda f: f.lon)
        lats = np.vectorize(lambda f: f.lat)

        # --- Browse and plot files ---
        longitudes = lons(flight.fixes)
        latitudes = lats(flight.fixes)
        flightPoints = list(zip(longitudes.tolist(), latitudes.tolist()))
    
        flightLineString = LineString(flightPoints)
        geoSeries = GeoSeries(flightLineString)
        
        # ----- Filter -----
        isFlightOK = True
        # Check that the flight time is > 45 min
        isDurationOk = flight.duration/60 >=45
        isFlightOK = isFlightOK and isDurationOk
        # Check that the flight is inside the France polygon
        if isFlightOK:
            isInsindeFrance = self.franceBoundingBox.contains(flightLineString)
            isFlightOK = isFlightOK and isInsindeFrance
        
        if isFlightOK:
                self.metaData.processedFlightsCount += 1
                self.axes = geoSeries.plot(ax=self.axes, 
                                            #figsize=CumulativeTrackBuilder.IMAGE_SIZE,
                                            linewidth=CumulativeTrackBuilder.LINE_WIDTH)

    def _dumpToFiles(self, fileObject=None):
        # Dump to HTML (results in a very large file)
        #mplleaflet.show(path='/Users/llauner/Downloads/latest-tracks.html')
        
        imageFullFileName = CumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + CumulativeTrackBuilder.TRACKS_IMAGE_FILE_NAME
        metadataFullFileName = CumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + CumulativeTrackBuilder.TRACKS_METADATA_FILE_NAME
        
        # Dump image
        if fileObject is None:      # Local file
            plt.savefig(imageFullFileName, format='png', 
                        dpi=CumulativeTrackBuilder.IMAGE_DPI, 
                        transparent=True, 
                        bbox_inches='tight',
                        interpolation='antialiasing', 
                        pad_inches=0)
        else:                       # File object
            plt.savefig(fileObject, format='png', dpi=CumulativeTrackBuilder.IMAGE_DPI, transparent=True, bbox_inches='tight',pad_inches=0)
        plt.close()
        
        # Build metadata
        tz = pytz.timezone('Europe/Paris')
        self.metaData.setEndTime(datetime.now(tz))
        
        # Dump MetaData
        if fileObject is None:      # Local file
            with open(metadataFullFileName, 'w') as jsonOut:
                jsonOut.write(self.JsonMetaData())


    def _dumpToFtp(self):
        imageFullFileName =  CumulativeTrackBuilder.TRACKS_IMAGE_FILE_NAME
        metadataFullFileName = CumulativeTrackBuilder.TRACKS_METADATA_FILE_NAME
        
        # Dump to FTP
        self.ftpClientOut = self.ftpClientOut.getFtpClient()
        # image
        fileBuffer = io.BytesIO()
        self._dumpToFiles(fileBuffer)
        fileBuffer.seek(0)
        
        print(f"Dump to FTP: {self.ftpClientOut.host} ->{imageFullFileName}")
        FtpHelper.dumpFileToFtp(self.ftpClientOut, 
                                        CumulativeTrackBuilder.FTP_TRACKS_ROOT_DIRECTORY, 
                                        imageFullFileName, 
                                        fileBuffer)
        fileBuffer.close()
        
        # metadata
        print(f"Dump to FTP: {self.ftpClientOut.host} ->{metadataFullFileName}")
        FtpHelper.dumpStringToFTP(self.ftpClientOut, 
                                        None, 
                                        metadataFullFileName, 
                                        self.JsonMetaData())
        
        # Print result so that it's logged
        print (self.JsonMetaData())


    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)
    
    def JsonMetaData(self):
        return self.metaData.toJSON()
    
# *******************************************************************************************
    class SlowBar(Bar):
        suffix='%(percent)d%% - %(index)d/%(max)d : %(currentMessage)s'
        @property
        def currentMessage(self):
            return self.progressMessage
        
        def __init__(self, *args, **kwargs):
            super(CumulativeTrackBuilder.SlowBar, self).__init__(*args, **kwargs)
            self.progressMessage = None
            
        def getLine(self):
            filled_length = int(self.width * self.progress)
            empty_length = self.width - filled_length

            message = self.message % self
            bar = self.fill * filled_length
            empty = self.empty_fill * empty_length
            suffix = self.suffix % self
            line = ''.join([message, self.bar_prefix, bar, empty, self.bar_suffix, suffix])
            return line
# *******************************************************************************************