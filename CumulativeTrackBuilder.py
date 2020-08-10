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
from progress.bar import Bar

from FtpHelper import *
from RunMetadata import RunMetadata
from RunStatistics import RunStatistics
from DumpFileName import DumpFileName


class CumulativeTrackBuilder:
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
    TRACKS_GEOJSON_ZIP_ARCHIVE_FILE_NAME = LATEST_PREFIX + \
        TRACKS_GEOJSON_ZIP_ARCHIVE_SUFFIX

    TRACKS_LOCAL_DUMP_DIRECTORY = "/Users/llauner/Downloads/"

    # --- Geo information ---41.196834, 10.328174
    FRANCE_BOUNDING_BOX = [(-6.566734, 51.722775), (10.645924,
                                                    51.726922), (10.328174, 41.196834), (-7.213631, 40.847787)]

    # --- Iamge settings ---
    IMAGE_DPI = 1800
    # IMAGE_SIZE=(20,20)
    LINE_WIDTH = 0.05

    # --- Dev and Debug ---
    NB_FILES_TO_KEEP = None

    def __init__(self, ftpClientIgc, ftpClientOut, targetYear=None, useLocalDirectory=False, isOutToLocalFiles=False):
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
        # Start on January 1st
        self.targetDate = date(self.targetYear, 1, 1)
        self.relDaysLookup = 365                   # All year

        self.franceBoundingBox = Polygon(self.FRANCE_BOUNDING_BOX)

        self.isRunningInCloud = not (
            self.useLocalDirectory and self.isOutToLocalFiles)

        # Statistics
        self.runStatistics = RunStatistics()

        # --- Setup plot ---
        mpl.rcParams['savefig.pad_inches'] = 0
        mpl.use('agg')
        figsize = None
        fig = plt.figure(figsize=figsize)
        self.axes = plt.axes([0, 0, 1, 1], frameon=False)
        self.axes.get_xaxis().set_visible(False)
        self.axes.get_yaxis().set_visible(False)
        plt.autoscale(tight=True)

        self.geojsonFeatures = []

    def run(self):
        if not self.useLocalDirectory:
            allFiles = FtpHelper.get_file_names_from_ftp(
                self.ftpClientIgc, self.targetDate, self.relDaysLookup)
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
            del allFiles[0: len(allFiles) -
                         CumulativeTrackBuilder.NB_FILES_TO_KEEP]

        # --- Process files to get flights
        if allFiles:
            progressBar = CumulativeTrackBuilder.SlowBar(
                'Processing', max=self.metaData.flightsCount)

            for i, filename in enumerate(allFiles):

                computedFileName = None
                flight = None
                # ----- File from FTP -----
                if not self.useLocalDirectory:
                    zip = FtpHelper.get_file_from_ftp(
                        self.ftpClientIgc, filename)

                    with zipfile.ZipFile(zip) as zip_file:
                        computedFileName = zip_file.filelist[0].filename
                        flight = igc_lib.Flight.create_from_zipfile(zip_file)

                # ----- File from local directory -----
                else:
                    flight = igc_lib.Flight.create_from_file(filename)

                if flight.date_timestamp:
                    flight_date = datetime.fromtimestamp(
                        flight.date_timestamp).date()
                # --- Build progress message and update
                if flight.valid and flight_date:
                    if computedFileName:
                        progressBar.progressMessage = "{} -> {}".format(
                            filename, computedFileName)
                    else:
                        progressBar.progressMessage = "{}".format(filename)
                else:
                    if computedFileName:
                        progressBar.progressMessage = "{} -> {} \t Discarded ! valid={}".format(
                            filename, computedFileName, flight.valid)
                    else:
                        progressBar.progressMessage = "{} \t Discarded ! valid={}".format(
                            filename, flight.valid)

                if self.isRunningInCloud:           # ProgressBar does not work in gcloud: pring on stdout
                    print(progressBar.getLine())

                progressBar.next()  # Update Progress

                # ----- Process flight -----
                self.createFlightImage(flight)
                self.createFlightGeoJson(flight)

            progressBar.finish()    # End Progress

            # --- Save bounding box ---
            self.metaData.boundingBoxUpperLeft = [
                self.axes.dataLim.extents[1], self.axes.dataLim.extents[0]]
            self.metaData.boundingBoxLowerRight = [
                self.axes.dataLim.extents[3], self.axes.dataLim.extents[2]]

        if self.isOutToLocalFiles:      # Dump to local files
            self._dumpToFiles()
        else:                           # Dump to FTP
            self._dumpToFtp()

    def createFlightImage(self, flight):
        """
        Creates transparent png image for given flight

        Args:
            flight ([type]): [description]
        """
        lons = np.vectorize(lambda f: f.lon)
        lats = np.vectorize(lambda f: f.lat)

        # --- Browse and plot files ---
        longitudes = lons(flight.fixes)
        latitudes = lats(flight.fixes)
        flightPoints = list(zip(longitudes.tolist(), latitudes.tolist()))

        flightLineString = LineString(flightPoints)
        geoSeries = GeoSeries(flightLineString)
        geoSeries.crs = "EPSG:3857"

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
            self.metaData.processedFlightsCount += 1
            self.axes = geoSeries.plot(ax=self.axes,
                                       # figsize=CumulativeTrackBuilder.IMAGE_SIZE,
                                       linewidth=CumulativeTrackBuilder.LINE_WIDTH)
            # Add flight to Statistics
            self.addFlightToStatistics(flight)

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
        flightLineString = LineString(flightPoints)

        # --- Reduce number of fixes ---
        fixes = np.array(flight.fixes)
        fixes = fixes[::10]
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
            feature = igc2geojson.get_geojson_feature_track_collection_simple(
                flight)
            self.geojsonFeatures.append(feature)

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
            geojsonFullFileName = CumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + \
                CumulativeTrackBuilder.TRACKS_GEOJSON_FILE_NAME
            igc2geojson.dumpFeaturesToFile(
                geojsonFullFileName, self.geojsonFeatures)

        # --- Dump to HTML (results in a very large file) ---
        # mplleaflet.show(path='/Users/llauner/Downloads/latest-tracks.html')

        # --- Dump image, metadata and statistics ---
        imageFullFileName = CumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + \
            CumulativeTrackBuilder.TRACKS_IMAGE_FILE_NAME
        metadataFullFileName = CumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + \
            CumulativeTrackBuilder.TRACKS_METADATA_FILE_NAME
        statisticsFullFileName = CumulativeTrackBuilder.TRACKS_LOCAL_DUMP_DIRECTORY + \
            CumulativeTrackBuilder.TRACKS_STATISTICS_FILE_NAME

        # --- Dump image ---
        if fileObject is None:      # Local file
            print(f"Dump image to file: {imageFullFileName}")
            plt.savefig(imageFullFileName, format='png',
                        dpi=CumulativeTrackBuilder.IMAGE_DPI,
                        transparent=True,
                        bbox_inches='tight',
                        interpolation='antialiasing',
                        pad_inches=0)
        else:                       # File object
            plt.savefig(fileObject,
                        format='png',
                        dpi=CumulativeTrackBuilder.IMAGE_DPI,
                        transparent=True,
                        bbox_inches='tight',
                        pad_inches=0)
        plt.close()

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

    def _getFilenamesforTargetYear(self, year):
        filenames = DumpFileName()

        filenames.TracksImageFilename = str(
            year) + CumulativeTrackBuilder.TRACKS_IMAGE_SUFFIX
        filenames.TracksMetadataFilename = str(
            year) + CumulativeTrackBuilder.TRACKS_METADATA_SUFFIX
        filenames.TracksStatisticsFilename = str(
            year) + CumulativeTrackBuilder.TRACKS_STATISTICS_SUFFIX
        filenames.TracksGeojsonFilename = str(
            year) + CumulativeTrackBuilder.TRACKS_GEOJSON_SUFFIX + ".geojson"
        filenames.TracksGeojsonZipFilename = str(
            year) + CumulativeTrackBuilder.TRACKS_GEOJSON_ZIP_ARCHIVE_SUFFIX
        return filenames

    def _dumpToFtp(self):
        """
        Dump items to files on FTP
        """
        latestFilenames = DumpFileName()
        latestFilenames.TracksImageFilename = CumulativeTrackBuilder.TRACKS_IMAGE_FILE_NAME
        latestFilenames.TracksMetadataFilename = CumulativeTrackBuilder.TRACKS_METADATA_FILE_NAME
        latestFilenames.TracksStatisticsFilename = CumulativeTrackBuilder.TRACKS_STATISTICS_FILE_NAME
        latestFilenames.TracksGeojsonFilename = CumulativeTrackBuilder.TRACKS_GEOJSON_FILE_NAME + ".geojson"
        latestFilenames.TracksGeojsonZipFilename = CumulativeTrackBuilder.TRACKS_GEOJSON_ZIP_ARCHIVE_FILE_NAME

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
                                  CumulativeTrackBuilder.FTP_TRACKS_ROOT_DIRECTORY,
                                  filenames.TracksGeojsonFilename,
                                  geojson)

        # Write ZIP to FTP
        # Create zip file
        fileBufferZip = io.BytesIO()
        zf = zipfile.ZipFile(fileBufferZip, mode="w",
                             compression=zipfile.ZIP_DEFLATED)
        zf.writestr(
            CumulativeTrackBuilder.TRACKS_GEOJSON_FILE_NAME_WITH_SUFFIX, geojson)
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

# *******************************************************************************************
    class SlowBar(Bar):
        suffix = '%(percent)d%% - %(index)d/%(max)d : %(currentMessage)s'

        @property
        def currentMessage(self):
            return self.progressMessage

        def __init__(self, *args, **kwargs):
            super(CumulativeTrackBuilder.SlowBar,
                  self).__init__(*args, **kwargs)
            self.progressMessage = None

        def getLine(self):
            filled_length = int(self.width * self.progress)
            empty_length = self.width - filled_length

            message = self.message % self
            bar = self.fill * filled_length
            empty = self.empty_fill * empty_length
            suffix = self.suffix % self
            line = ''.join([message, self.bar_prefix, bar,
                            empty, self.bar_suffix, suffix])
            return line
# *******************************************************************************************
