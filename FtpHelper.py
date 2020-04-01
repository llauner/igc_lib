
import ftplib
import pathlib
import os
import zipfile

from io import BytesIO

from datetime import datetime, date, time, timedelta
from dateutil import parser

LOCAL_IGC_DIRECTORY = "/Users/llauner/Downloads/igc/Netcoupe/"


class FtpHelper():

    @staticmethod
    def get_file_names_from_ftp(ftp_client, target_date, relDaysLookup):
        '''
        Get filenames to be retrieved from FTP
        Args:
            ftp_client: a valid ftp client
            target_date: only files having this modified date will be retrieved
            relDaysLookup: number of days after/before the target_date to retrieve files from
        '''
        file_names = []

        lookup_date = target_date
        search_start_date = target_date
        search_end_date = target_date

        if relDaysLookup:
            lookup_date = target_date + timedelta(days=relDaysLookup)

        if lookup_date > target_date:
            search_start_date = target_date
            search_end_date = lookup_date
        else:
            search_start_date = lookup_date
            search_end_date = target_date

        files = ftp_client.mlsd()   # List files from FTP

        for file in files:
            name = file[0]
            suffix = pathlib.Path(name).suffix.replace('.','')
            timestamp = file[1]['modify']
            modified_date = parser.parse(timestamp).date()
        
            if suffix == "zip" and (search_start_date <= modified_date and modified_date <= search_end_date):
                file_names.append(name)

        return file_names

    @staticmethod
    def get_ftp_client(ftp_server, ftp_username, ftp_password):
        ftp_client = ftplib.FTP(ftp_server, ftp_username, ftp_password)
        return ftp_client

    @staticmethod
    def get_file_from_ftp(ftp_client, filename):
        r = BytesIO()
        ftp_client.retrbinary('RETR ' + filename, r.write)
        return r
    
    # ---------------------------- Local directory -------------------------
    @staticmethod
    def getFIlenamesFromLocalFolder():
        files = []
        # r=root, d=directories, f = files
        for r, d, f in os.walk(LOCAL_IGC_DIRECTORY):
            for file in f:
                if '.igc' in file:
                    files.append(os.path.join(r, file))
        return files
    
    # ------------------------ Utility ------------------------------------
    @staticmethod
    def leechFilesFromFtp(ftp_client, target_date, relDaysLookup):
        allFiles = FtpHelper.get_file_names_from_ftp(ftp_client, target_date, relDaysLookup)
        
        if allFiles:
            for i,filename in enumerate(allFiles):
                computedFileName = None
                # ----- File from FTP -----
                zip = FtpHelper.get_file_from_ftp(ftp_client, filename)
                with zipfile.ZipFile(zip) as zip_file:
                    zip_file.extractall("/Users/llauner/Downloads/igc/2020")
    