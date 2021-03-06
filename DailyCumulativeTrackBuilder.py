from datetime import date, datetime
import pytz
import json
import zipfile
import igc_lib
import igc2geojson
import numpy as np
import zipfile
import ntpath

import io

import matplotlib.pyplot as plt
import matplotlib as mpl
from shapely.geometry import Point, LineString, Polygon
import geopandas as gpd
from geopandas import GeoDataFrame, GeoSeries
import pandas as pd
import mplleaflet

from StorageService import *
from FtpHelper import *
from RunMetadataTracks import RunMetadataTracks
from DailyRunStatistics import DailyRunStatistics
from DumpFileName import DumpFileName
from FtpHelper import FtpHelper


class DailyCumulativeTrackBuilder:

    @property
    def ProcessedFileList(self):
        return self.fileList

    @property
    def IncludeFranceOnly(self):
        return False

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
        self.metaData = RunMetadataTracks(target_date)
        self.ftpServerCredentials = ftpServerCredentials
        self.ftpClientOut = None
        self.target_date = target_date.strftime('%Y_%m_%d')
        self.storageService = StorageService(self.target_date)
        self.isOutToLocalFiles = isOutToLocalFiles

        self.fileList = fileList
        self.flightsCount = None

        self.franceBoundingBox = Polygon(self.FRANCE_BOUNDING_BOX)

        # Statistics
        self.runStatistics = DailyRunStatistics()
        self.geojsonFeatures = []

    def run_alternative_source(self, source_folder):

        # --- Get file list to process
        self.storageService = StorageService(self.target_date, StorageService.Trace_aggregator_alternative_source_bucket_name)
        self.fileList =  self.storageService.GetFileListForAlternativeSource(source_folder)
        
        # --- Process file list
        self.process_file_list()
        self.update_metadata()

        # --- Dump result
        self.dump_to_bucket_and_clean(source_folder)

    def run(self):
        self.metaData.flightsCount = len(self.fileList)

        # --- Process file list
        self.process_file_list()
        self.update_metadata()

        # --- Dump result
        if self.isOutToLocalFiles:      # Dump to local files
            self._dumpToFiles()
        else:                           # Dump to FTP
            self._dumpToFtp()

        return self.metaData


    def process_file_list(self) :
        self.metaData.flightsCount = len(self.fileList)

        # --- Process files to get flights
        if self.fileList:
            for i, filename in enumerate(self.fileList):
                file_as_bytesio = self.storageService.GetFileAsString(filename)

                flight = igc_lib.Flight.create_from_bytesio(file_as_bytesio)
                file_as_bytesio.close
                del file_as_bytesio

                if flight.date_timestamp:
                    flight_date = datetime.fromtimestamp(flight.date_timestamp).date()

                # --- Build progress message and update
                if not (i % 5):
                    print(f"{i}/{self.metaData.flightsCount} : {filename}")
                    

                # ----- Process flight -----
                flightId = self.get_netcoupe_flight_id_from_filename(filename)
                self.createFlightGeoJson(flight, flightId)
                
                del flight


    def createFlightGeoJson(self, flight, flightId = None):
        """
        Create GeoJson for given flight

        Args:
            flight ([type]): [description]
        """

        # --- Inside France detection ---
        isInsindeFrance = True
        if self.IncludeFranceOnly:
            lons = np.vectorize(lambda f: f.lon)
            lats = np.vectorize(lambda f: f.lat)

            longitudes = lons(flight.fixes)
            latitudes = lats(flight.fixes)
            # Remove points
            longitudes = longitudes[::50]
            latitudes = latitudes[::50]

            flightPoints = list(zip(longitudes.tolist(), latitudes.tolist()))
            if len(flightPoints) < 2:
                return

            flightLineString = LineString(flightPoints)
            isInsindeFrance = self.franceBoundingBox.contains(flightLineString)

            del lons
            del lats
            del longitudes
            del latitudes
            del flightPoints
            del flightLineString

        # --- Reduce number of fixes ---
        fixes = np.array(flight.fixes)
        del flight.fixes

        fixes = fixes[::4]
        flight.fixes = fixes.tolist()

        # ----- Filter -----
        # Check that the flight time is > 45 min
        isDurationOk = flight.duration/60 >= 45
        isFlightOK = flight.valid and isDurationOk and isInsindeFrance

        if isFlightOK:
            feature = igc2geojson.get_geojson_feature_track_collection_simple(flight, flightId)
            self.geojsonFeatures.append(feature)

            self.metaData.processedFlightsCount += 1
            self.runStatistics.flightsCount += 1
            del feature
        else:
            print(f"File Discarded: isDurationOk={isDurationOk}")


    def update_metadata(self):
        # --- Build metadata ----
        tz = pytz.timezone('Europe/Paris')
        self.metaData.script_end_time= datetime.now(tz)
       

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
        self.metaData.script_end_time= datetime.now(tz)

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
        latestFilenames.TracksGeojsonFilename = DailyCumulativeTrackBuilder.TRACKS_GEOJSON_FILE_NAME_WITH_SUFFIX.format(self.target_date)
        latestFilenames.TracksGeojsonZipFilename = DailyCumulativeTrackBuilder.TRACKS_GEOJSON_ZIP_ARCHIVE_FILE_NAME.format(self.target_date)

        # -- Write to FTP
        self._write_to_FTP_and_gcp_bucket(latestFilenames)

        # Print result so that it's logged
        print(self.JsonMetaData())

        # Disconnect FTP
        self.ftpClientOut.close()

    def _write_to_FTP_and_gcp_bucket(self, filenames):
        # Dump to FTP

        # --- Dump geojson ---
        geojson = igc2geojson.getJsonFromFeatures(self.geojsonFeatures)
        
        # Write ZIP to FTP
        # Create zip file
        fileBufferZip = io.BytesIO()
        zf = zipfile.ZipFile(fileBufferZip, mode="w",
                             compression=zipfile.ZIP_DEFLATED)
        zf.writestr(filenames.TracksGeojsonFilename, geojson)
        zf.close()
        fileBufferZip.seek(0)

        # Upload to GCP Bucket
        print( f"Dump ZIP to GCP Storage Bucket: {StorageService.Trace_aggregator_bucket_name} / {StorageService.Trace_aggregator_backlog_folder_name} / {filenames.TracksGeojsonZipFilename}")
        self.storageService.UploadFileToBucket(fileBufferZip, StorageService.Trace_aggregator_bucket_name, StorageService.Trace_aggregator_backlog_folder_name, filenames.TracksGeojsonZipFilename)
        fileBufferZip.seek(0)

        # Write to FTP
        print( f"Dump ZIP to FTP: { self.ftpClientOut.host} ->{filenames.TracksGeojsonZipFilename}")
        FtpHelper.dumpFileToFtp( self.ftpClientOut,
                                DailyCumulativeTrackBuilder.FTP_TRACKS_ROOT_DIRECTORY,
                                filenames.TracksGeojsonZipFilename,
                                fileBufferZip)

        fileBufferZip.close()

        
        # metadata
        print( f"Dump Metadata to FTP: { self.ftpClientOut.host} ->{filenames.TracksMetadataFilename}")
        FtpHelper.dumpStringToFTP(self.ftpClientOut,
                                  None,
                                  filenames.TracksMetadataFilename,
                                  self.JsonMetaData())

        self.ftpClientOut.quit()

        del geojson
        del self.geojsonFeatures

    def dump_to_bucket_and_clean(self, source_folder):
        filenames = DumpFileName()
        filenames.TracksMetadataFilename = DailyCumulativeTrackBuilder.TRACKS_METADATA_FILE_NAME.format(source_folder)
        filenames.TracksGeojsonFilename = DailyCumulativeTrackBuilder.TRACKS_GEOJSON_FILE_NAME_WITH_SUFFIX.format(source_folder)
        filenames.TracksGeojsonZipFilename = DailyCumulativeTrackBuilder.TRACKS_GEOJSON_ZIP_ARCHIVE_FILE_NAME.format(source_folder)

        # --- Dump geojson ---
        geojson = igc2geojson.getJsonFromFeatures(self.geojsonFeatures)

        # Create zip file
        fileBufferZip = io.BytesIO()
        zf = zipfile.ZipFile(fileBufferZip, mode="w", compression=zipfile.ZIP_DEFLATED)
        zf.writestr(filenames.TracksGeojsonFilename, geojson)
        zf.close()

        # Upload to GCP Bucket
        print( f"Dump ZIP to GCP Storage Bucket: {StorageService.Trace_aggregator_alternative_source_bucket_name} / {source_folder} / {filenames.TracksGeojsonZipFilename}")
        self.storageService.UploadFileToBucket(fileBufferZip, StorageService.Trace_aggregator_alternative_source_bucket_name, source_folder, filenames.TracksGeojsonZipFilename)
        fileBufferZip.close()

        # Metadata
        print( f"Dump Metadata to GCP Storage Bucket: {StorageService.Trace_aggregator_alternative_source_bucket_name} / {source_folder} / {filenames.TracksMetadataFilename}")
        jsonMetaData = self.JsonMetaData()
        self.storageService.UploadStringToBucket(jsonMetaData, StorageService.Trace_aggregator_alternative_source_bucket_name, source_folder, filenames.TracksMetadataFilename)                

        # Delet igc files from bucket
        self.storageService.DeleteFilesFromBucket(self.fileList, StorageService.Trace_aggregator_alternative_source_bucket_name)


    def get_netcoupe_flight_id_from_filename(self, filename):
        '''
        Try to extract the flightId from the filename.
        filename has the form: NetCoupeYYYY_{flightId}.igc
        Return None if cannot extract
        '''
        filename = ntpath.basename(filename)
        first = '_'
        last = '.igc'
        try:
            start = filename.index( first ) + len( first )
            end = filename.index( last, start )
            return filename[start:end]
        except ValueError:
            return None

    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)

    def JsonMetaData(self):
        return self.metaData.toJSON()

