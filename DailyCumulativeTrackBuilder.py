from datetime import date, datetime
import pytz
import json
import zipfile
import igc_lib
import igc2geojson
import numpy as np
import zipfile

import io

import matplotlib.pyplot as plt
import matplotlib as mpl
from shapely.geometry import Point, LineString, Polygon
import geopandas as gpd
from geopandas import GeoDataFrame, GeoSeries
import pandas as pd
import mplleaflet
from tqdm import tqdm

from StorageService import *
from FtpHelper import *
from RunMetadata import RunMetadata
from RunStatistics import RunStatistics
from DumpFileName import DumpFileName
from FtpHelper import FtpHelper


class DailyCumulativeTrackBuilder:

    @property
    def ProcessedFileList(self):
        return self.fileList

    # --- Files dans folders ---
    FTP_TRACKS_ROOT_DIRECTORY = "tracemap/tracks/"

    TRACKS_STATISTICS_FILE_NAME = "{0}-tracks-statistics.json"

    TRACKS_METADATA_FILE_NAME = "{0}-tracks-metadata.json"

    TRACKS_GEOJSON_FILE_NAME = "{0}-tracks"
    TRACKS_GEOJSON_FILE_NAME_WITH_SUFFIX = TRACKS_GEOJSON_FILE_NAME + ".geojson"

    TRACKS_GEOJSON_ZIP_ARCHIVE_FILE_NAME = "{0}-tracks.geojson.zip"

    TRACKS_LOCAL_DUMP_DIRECTORY = "D:\\llauner\src\\cumulativeTracksWeb\\tracks\\"

    # --- Geo information ---41.196834, 10.328174
    FRANCE_BOUNDING_BOX = [(-6.566734, 51.722775), (10.645924,51.726922), (10.328174, 41.196834), (-7.213631, 40.847787)]

    def __init__(self, ftpServerCredentials, target_date, fileList=None, isOutToLocalFiles=False):
        self.metaData = RunMetadata()
        self.ftpServerCredentials = ftpServerCredentials
        self.ftpClientOut = None
        self.target_date = target_date.strftime('%Y_%m_%d')
        self.storageService = StorageService(self.target_date)
        self.isOutToLocalFiles = isOutToLocalFiles

        self.fileList = fileList
        self.flightsCount = None

        self.progressMessage = None

        self.franceBoundingBox = Polygon(self.FRANCE_BOUNDING_BOX)

        self.isRunningInCloud = not self.isOutToLocalFiles

        # Statistics
        self.runStatistics = RunStatistics()
        self.geojsonFeatures = []

    def run(self):
        self.metaData.flightsCount = len(self.fileList)

        # --- Process files to get flights
        if self.fileList:
            bar = tqdm(total=self.metaData.flightsCount)

            for i, filename in enumerate(self.fileList):
                file_as_bytesio = self.storageService.GetFileAsString(filename)

                flight = igc_lib.Flight.create_from_bytesio(file_as_bytesio)
                file_as_bytesio.close
                del file_as_bytesio

                if flight.date_timestamp:
                    flight_date = datetime.fromtimestamp(flight.date_timestamp).date()

                # --- Build progress message and update
                if not (i % 5):
                    msg = f"{i}/{self.metaData.flightsCount} : {filename}"
                    bar.set_description(msg)
                    bar.write(msg)
                    bar.update(i)  # Update Progress

                # ----- Process flight -----
                self.createFlightGeoJson(flight)
                
                del flight

            bar.close()    # End Progress


        if self.isOutToLocalFiles:      # Dump to local files
            self._dumpToFiles()
        else:                           # Dump to FTP
            self._dumpToFtp()

        return self.metaData

    def createFlightGeoJson(self, flight):
        """
        Create GeoJson for given flight

        Args:
            flight ([type]): [description]
        """
        lons = np.vectorize(lambda f: f.lon)
        lats = np.vectorize(lambda f: f.lat)

        # --- Browse and plot files ---
        longitudes = lons(flight.fixes)
        latitudes = lats(flight.fixes)
        # Remove points
        longitudes = longitudes[::10]
        latitudes = latitudes[::10]

        flightPoints = list(zip(longitudes.tolist(), latitudes.tolist()))
        if len(flightPoints) < 2:
            return

        flightLineString = LineString(flightPoints)

        geoSeries = GeoSeries(flightLineString)
        geoSeries.crs = "EPSG:3857"

        # --- Reduce number of fixes ---
        fixes = np.array(flight.fixes)
        del flight.fixes

        fixes = fixes[::100]
        flight.fixes = fixes.tolist()

        # ----- Filter -----
        isFlightOK = True
        # Check that the flight time is > 45 min
        isDurationOk = flight.duration/60 >= 45
        isFlightOK = isFlightOK and isDurationOk
        # Check that the flight is inside the France polygon
        if isFlightOK:
            isInsindeFrance = self.franceBoundingBox.contains(flightLineString)
            isFlightOK = isFlightOK and isInsindeFrance

        if isFlightOK:
            feature = igc2geojson.get_geojson_feature_track_collection_simple(flight)
            self.geojsonFeatures.append(feature)

            self.metaData.processedFlightsCount += 1
            self.addFlightToStatistics(flight)
            del feature

        del lons
        del lats
        del longitudes
        del latitudes
        del flightPoints
        del flightLineString
        del geoSeries


    def addFlightToStatistics(self, flight):
        flightDate = datetime.fromtimestamp(flight.date_timestamp).date()
        flightDate = flightDate.strftime('%Y_%m_%d')
        self.runStatistics.addTimeSeries(flightDate)

    def _dumpToFiles(self):
        """
        Dump items to files
        """
        # --- Dump geojson file ---
        geojsonFullFileName =   DailyCumulativeTrackBuilder.TRACKS_GEOJSON_FILE_NAME.format(DailyCumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + self.target_date)
        igc2geojson.dumpFeaturesToFile(geojsonFullFileName, self.geojsonFeatures)

        # --- Dump image, metadata and statistics ---    
        metadataFullFileName = DailyCumulativeTrackBuilder.TRACKS_METADATA_FILE_NAME.format(DailyCumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + self.target_date)
        statisticsFullFileName =  DailyCumulativeTrackBuilder.TRACKS_STATISTICS_FILE_NAME.format(DailyCumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + self.target_date)

        # --- Build metadata ----
        tz = pytz.timezone('Europe/Paris')
        self.metaData.setEndTime(datetime.now(tz))

        # Dump MetaData
        print(f"Dump metadata to file: {metadataFullFileName}")
        with open(metadataFullFileName, 'w') as jsonOut:
            jsonOut.write(self.JsonMetaData())

        # --- Dump Statistics ---
        print(f"Dump statistics to file: {statisticsFullFileName}")
        statistics = self.runStatistics.toJson()
        with open(statisticsFullFileName, 'w') as jsonOut:
            jsonOut.write(statistics)


    def _dumpToFtp(self):
        """
        Dump items to files on FTP
        """
        self.ftpClientOut = FtpHelper.get_ftp_client(self.ftpServerCredentials.ServerName, self.ftpServerCredentials.Login, self.ftpServerCredentials.Password)

        latestFilenames = DumpFileName()
        latestFilenames.TracksMetadataFilename = DailyCumulativeTrackBuilder.TRACKS_METADATA_FILE_NAME.format(self.target_date)
        latestFilenames.TracksStatisticsFilename = DailyCumulativeTrackBuilder.TRACKS_STATISTICS_FILE_NAME.format(self.target_date)
        latestFilenames.TracksGeojsonFilename = DailyCumulativeTrackBuilder.TRACKS_GEOJSON_FILE_NAME_WITH_SUFFIX.format(self.target_date)
        latestFilenames.TracksGeojsonZipFilename = DailyCumulativeTrackBuilder.TRACKS_GEOJSON_ZIP_ARCHIVE_FILE_NAME.format(self.target_date)

        # -- Write to FTP
        self._writeToFTP(latestFilenames)

        # Print result so that it's logged
        print(self.JsonMetaData())

        # Disconnect FTP
        self.ftpClientOut.close()

    def _writeToFTP(self, filenames):
        # Dump to FTP

        # --- Dump geojson ---
        # Write geojson to FTP
        print(f"Dump to FTP: {self.ftpClientOut.host} ->{filenames.TracksGeojsonFilename}")
        geojson = igc2geojson.getJsonFromFeatures(self.geojsonFeatures)
        FtpHelper.dumpStringToFTP( self.ftpClientOut,
                                  DailyCumulativeTrackBuilder.FTP_TRACKS_ROOT_DIRECTORY,
                                  filenames.TracksGeojsonFilename,
                                  geojson)

        # Write ZIP to FTP
        # Create zip file
        fileBufferZip = io.BytesIO()
        zf = zipfile.ZipFile(fileBufferZip, mode="w",
                             compression=zipfile.ZIP_DEFLATED)
        zf.writestr(filenames.TracksGeojsonFilename, geojson)
        zf.close()
        fileBufferZip.seek(0)

        # Write to FTP
        print( f"Dump ZIP to FTP: { self.ftpClientOut.host} ->{filenames.TracksGeojsonZipFilename}")
        FtpHelper.dumpFileToFtp( self.ftpClientOut,
                                None,
                                filenames.TracksGeojsonZipFilename,
                                fileBufferZip)
        fileBufferZip.close()


        # metadata
        print(f"Dump to FTP: { self.ftpClientOut.host} ->{filenames.TracksMetadataFilename}")
        FtpHelper.dumpStringToFTP( self.ftpClientOut,
                                  None,
                                  filenames.TracksMetadataFilename,
                                  self.JsonMetaData())

        # --- Dump Statistics ---
        print(f"Dump to FTP: { self.ftpClientOut.host} ->{filenames.TracksStatisticsFilename}")
        FtpHelper.dumpStringToFTP( self.ftpClientOut,
                                  None,
                                  filenames.TracksStatisticsFilename,
                                  self.runStatistics.toJson())

        self.ftpClientOut.quit()

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)

    def JsonMetaData(self):
        return self.metaData.toJSON()

