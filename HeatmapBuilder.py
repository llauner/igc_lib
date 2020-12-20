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

from StorageService import *
from FtpHelper import *
from RunMetadata import RunMetadata
from DumpFileName import HeatmapDumpFileName
from FtpHelper import FtpHelper


class HeatmapBuilder:

    @property
    def ProcessedFileList(self):
        return self.fileList

    @property
    def IncludeFranceOnly(self):
        return False

    # --- Files dans folders ---
    FTP_GEOJSON_ROOT_DIRECTORY = "heatmap/geojson/"
    GEOJSON_METADATA_FILE_NAME = "{0}-metadata.json"
    HEATMAP_GEOJSON_FILE_NAME = "{0}-heatmap.geojson"


    def __init__(self, ftpServerCredentials, target_date, fileList):
        self.metaData = RunMetadata(target_date)
        self.ftpServerCredentials = ftpServerCredentials
        self.ftpClientOut = None

        self.target_date = target_date
        self.str_target_date = self.target_date.strftime('%Y_%m_%d')
        self.storageService = StorageService(self.target_date)


        self.fileList = fileList
        self.flightsCount = len(self.fileList)
        self.global_thermals = []

    def run(self):
         # --- Process files to get flights
        if self.fileList:
            for i, filename in enumerate(self.fileList):
                file_as_bytesio = self.storageService.GetFileAsString(filename)

                flight = igc_lib.Flight.create_from_bytesio(file_as_bytesio)
                file_as_bytesio.close
                del file_as_bytesio

                if flight.valid:
                    self.flightsCount += 1
                    for t in flight.thermals:
                        t.exit_fix.flight = None
                        t.enter_fix.flight = None
                    self.global_thermals.extend(flight.thermals)
                    print(f"{i+1}/{self.flightsCount} :{filename} -> Thermals#={len(flight.thermals)}")
                else:
                    print(f"{i+1}/{self.flightsCount} :{filename} -> Discarded ! valid={flight.valid} date={self.str_target_date}")
                del flight

            # Metadata
            tz = pytz.timezone('Europe/Paris')
            self.metaData.setEndTime(datetime.now(tz))
            self.metaData.flightsCount = len(self.fileList)
            self.metaData.processedFlightsCount = self.flightsCount
            self.metaData.thermalsCount = len(self.global_thermals)

            # Dump to FTP
            self._dumpToFtp()

        return self.metaData.toJSON()

    def _dumpToFtp(self):
        """
        Dump items to files on FTP
        """
        self.ftpClientOut = FtpHelper.get_ftp_client(self.ftpServerCredentials.ServerName, self.ftpServerCredentials.Login, self.ftpServerCredentials.Password)

        latestFilenames = HeatmapDumpFileName()
        latestFilenames.TracksMetadataFilename = HeatmapBuilder.GEOJSON_METADATA_FILE_NAME.format(self.target_date)
        latestFilenames.HeatmapGeojsonFilename = HeatmapBuilder.HEATMAP_GEOJSON_FILE_NAME.format(self.target_date)
    

        # -- Write to FTP
        self._writeToFTP(latestFilenames)

        # Print result so that it's logged
        print(self.metaData.toJSON())

        # Disconnect FTP
        self.ftpClientOut.close()

    def _writeToFTP(self, filenames):
        # Dump to FTP

        # --- Dump geojson ---
        igc2geojson.dump_to_ftp(self.ftpClientOut, HeatmapBuilder.FTP_GEOJSON_ROOT_DIRECTORY, filenames.HeatmapGeojsonFilename, self.global_thermals)
        print(f"GeoJson output to FTP: {self.ftpClientOut.host} -> { filenames.HeatmapGeojsonFilename}")

        # --- Dump metadata ---
        print(f"Dump to FTP: {self.ftpClientOut.host} ->{filenames.HeatmapMetadataFilename}")
        FtpHelper.dumpStringToFTP(self.ftpClientOut,
                                  None,
                                  filenames.TracksMetadataFilename,
                                  self.metaData.toJSON())

        self.ftpClientOut.quit()


    def toJSON(self):
        return json.dumps(self, default=lambda o: o.__dict__, sort_keys=True)


