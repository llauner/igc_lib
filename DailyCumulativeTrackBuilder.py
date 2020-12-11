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
import progressbar

from StorageService import *
from FtpHelper import *
from RunMetadata import RunMetadata
from RunStatistics import RunStatistics
from DumpFileName import DumpFileName


class DailyCumulativeTrackBuilder:
    # --- Files dans folders ---
    FTP_TRACKS_ROOT_DIRECTORY = "tracemap/tracks/"
    LATEST_PREFIX = "latest"

    TRACKS_IMAGE_SUFFIX = "-tracks.png"
    TRACKS_IMAGE_FILE_NAME = LATEST_PREFIX + TRACKS_IMAGE_SUFFIX

    TRACKS_STATISTICS_SUFFIX = "-tracks-statistics.json"
    TRACKS_STATISTICS_FILE_NAME = LATEST_PREFIX + TRACKS_STATISTICS_SUFFIX

    TRACKS_METADATA_SUFFIX = "-tracks-metadata.json"
    TRACKS_METADATA_FILE_NAME = LATEST_PREFIX + TRACKS_METADATA_SUFFIX

    TRACKS_GEOJSON_SUFFIX = "-tracks"
    TRACKS_GEOJSON_FILE_NAME = LATEST_PREFIX + TRACKS_GEOJSON_SUFFIX
    TRACKS_GEOJSON_FILE_NAME_WITH_SUFFIX = TRACKS_GEOJSON_FILE_NAME + ".geojson"

    TRACKS_GEOJSON_ZIP_ARCHIVE_SUFFIX = "-tracks.geojson.zip"
    TRACKS_GEOJSON_ZIP_ARCHIVE_FILE_NAME = LATEST_PREFIX + TRACKS_GEOJSON_ZIP_ARCHIVE_SUFFIX

    TRACKS_LOCAL_DUMP_DIRECTORY = "D:\\llauner\src\\cumulativeTracksWeb\\tracks\\"

    # --- Geo information ---41.196834, 10.328174
    FRANCE_BOUNDING_BOX = [(-6.566734, 51.722775), (10.645924,51.726922), (10.328174, 41.196834), (-7.213631, 40.847787)]

    def __init__(self, ftpClientOut, target_date, isOutToLocalFiles=False):
        self.metaData = RunMetadata()
        self.ftpClientOut = ftpClientOut
        self.storageService = StorageService()
        self.target_date = target_date
        self.isOutToLocalFiles = isOutToLocalFiles

        self.fileList = []
        self.flightsCount = None

        self.progressMessage = None

        self.franceBoundingBox = Polygon(self.FRANCE_BOUNDING_BOX)

        self.isRunningInCloud = not self.isOutToLocalFiles

        # Statistics
        self.runStatistics = RunStatistics()
        self.geojsonFeatures = []

    def run(self):
        self.fileList  = self.storageService.GetFileList(self.target_date)
        self.fileList = self.fileList[::30]
        self.metaData.flightsCount = len(self.fileList)

        # --- Process files to get flights
        if self.fileList:
            bar = progressbar.ProgressBar(max_value=self.metaData.flightsCount)

            for i, filename in enumerate(self.fileList):
                file_as_bytesio = self.storageService.GetFileAsString(filename)

                flight = igc_lib.Flight.create_from_bytesio(file_as_bytesio)
                file_as_bytesio.close
                del file_as_bytesio

                if flight.date_timestamp:
                    flight_date = datetime.fromtimestamp(flight.date_timestamp).date()
                # --- Build progress message and update
                if flight.valid and flight_date:
                    bar.progressMessage = "{}".format(filename)
                else:
                    bar.progressMessage = "{} \t Discarded ! valid={}".format(filename, flight.valid)

                if self.isRunningInCloud:           # ProgressBar does not work in gcloud: pring on stdout
                    print(bar.getLine())

                bar.update(i)  # Update Progress

                # ----- Process flight -----
                self.createFlightGeoJson(flight)
                
                del flight

            bar.finish()    # End Progress


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
        longitudes = longitudes[::100]
        latitudes = latitudes[::100]

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

    def _dumpToFiles(self, fileObject=None):
        """
        Dump items to files

        Args:
            fileObject ([type], optional): [description]. Defaults to None.
        """
        # --- Dump geojson file ---
        if fileObject is None:      # Local file
            geojsonFullFileName = DailyCumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + DailyCumulativeTrackBuilder.TRACKS_GEOJSON_FILE_NAME
            igc2geojson.dumpFeaturesToFile(geojsonFullFileName, self.geojsonFeatures)

        # --- Dump image, metadata and statistics ---    
        metadataFullFileName = DailyCumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + DailyCumulativeTrackBuilder.TRACKS_METADATA_FILE_NAME
        statisticsFullFileName = DailyCumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + DailyCumulativeTrackBuilder.TRACKS_STATISTICS_FILE_NAME

        # --- Build metadata ----
        tz = pytz.timezone('Europe/Paris')
        self.metaData.setEndTime(datetime.now(tz))

        # Dump MetaData
        if fileObject is None:      # Local file
            print(f"Dump metadata to file: {metadataFullFileName}")
            with open(metadataFullFileName, 'w') as jsonOut:
                jsonOut.write(self.JsonMetaData())

        # --- Dump Statistics ---
        if fileObject is None:      # Local file
            print(f"Dump statistics to file: {statisticsFullFileName}")
            statistics = self.runStatistics.toJson()
            with open(statisticsFullFileName, 'w') as jsonOut:
                jsonOut.write(statistics)


    def _dumpToFtp(self):
        """
        Dump items to files on FTP
        """
        latestFilenames = DumpFileName()
        latestFilenames.TracksImageFilename = DailyCumulativeTrackBuilder.TRACKS_IMAGE_FILE_NAME
        latestFilenames.TracksMetadataFilename = DailyCumulativeTrackBuilder.TRACKS_METADATA_FILE_NAME
        latestFilenames.TracksStatisticsFilename = DailyCumulativeTrackBuilder.TRACKS_STATISTICS_FILE_NAME
        latestFilenames.TracksGeojsonFilename = DailyCumulativeTrackBuilder.TRACKS_GEOJSON_FILE_NAME + ".geojson"
        latestFilenames.TracksGeojsonZipFilename = DailyCumulativeTrackBuilder.TRACKS_GEOJSON_ZIP_ARCHIVE_FILE_NAME

        # --- Test end of year and copy files if necessary
        isLastRunOfYear = False
        if self.metaData.script_start_time.month == 12 and self.metaData.script_start_time.day >= 20:
            isLastRunOfYear = True
            filenamesForYear = self._getFilenamesforTargetYear(2019)

        # -- Write to FTP
        self._writeToFTP(latestFilenames)
        if (isLastRunOfYear):
            self._writeToFTP(filenamesForYear)

        # Print result so that it's logged
        print(self.JsonMetaData())

    def _writeToFTP(self, filenames):
        # Dump to FTP
        ftpClientOut = self.ftpClientOut.getFtpClient()

        # --- Dump geojson ---
        # Write geojson to FTP
        print(
            f"Dump to FTP: {ftpClientOut.host} ->{filenames.TracksGeojsonFilename}")
        geojson = igc2geojson.getJsonFromFeatures(self.geojsonFeatures)
        FtpHelper.dumpStringToFTP(ftpClientOut,
                                  DailyCumulativeTrackBuilder.FTP_TRACKS_ROOT_DIRECTORY,
                                  filenames.TracksGeojsonFilename,
                                  geojson)

        # Write ZIP to FTP
        # Create zip file
        fileBufferZip = io.BytesIO()
        zf = zipfile.ZipFile(fileBufferZip, mode="w",
                             compression=zipfile.ZIP_DEFLATED)
        zf.writestr(
            DailyCumulativeTrackBuilder.TRACKS_GEOJSON_FILE_NAME_WITH_SUFFIX, geojson)
        zf.close()
        fileBufferZip.seek(0)

        # Write to FTP
        print(
            f"Dump ZIP to FTP: {ftpClientOut.host} ->{filenames.TracksGeojsonZipFilename}")
        FtpHelper.dumpFileToFtp(ftpClientOut,
                                None,
                                filenames.TracksGeojsonZipFilename,
                                fileBufferZip)
        fileBufferZip.close()

        # image
        fileBuffer = io.BytesIO()
        self._dumpToFiles(fileBuffer)
        fileBuffer.seek(0)

        print(
            f"Dump to FTP: {ftpClientOut.host} ->{filenames.TracksImageFilename}")
        FtpHelper.dumpFileToFtp(ftpClientOut,
                                None,
                                filenames.TracksImageFilename,
                                fileBuffer)
        fileBuffer.close()

        # metadata
        print(
            f"Dump to FTP: {ftpClientOut.host} ->{filenames.TracksMetadataFilename}")
        FtpHelper.dumpStringToFTP(ftpClientOut,
                                  None,
                                  filenames.TracksMetadataFilename,
                                  self.JsonMetaData())

        # --- Dump Statistics ---
        print(
            f"Dump to FTP: {ftpClientOut.host} ->{filenames.TracksStatisticsFilename}")
        FtpHelper.dumpStringToFTP(ftpClientOut,
                                  None,
                                  filenames.TracksStatisticsFilename,
                                  self.runStatistics.toJson())

        ftpClientOut.quit()

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)

    def JsonMetaData(self):
        return self.metaData.toJSON()

