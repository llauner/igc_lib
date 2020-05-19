
import ftplib
import pathlib
import os
import zipfile

from io import BytesIO

from datetime import datetime, date, time, timedelta
from dateutil import parser

LOCAL_IGC_DIRECTORY = "/Users/llauner/Downloads/igc/Netcoupe/"


class FtpHelper():
    
    def __init__(self, host, login, password):
        self.host = host
        self.login = login
        self.password = password
        
    def getFtpClient(self):
        return FtpHelper.get_ftp_client(self.host, self.login, self.password)

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
            
        lines = []
        ftp_client.dir(".", lines.append)

        for line in lines:
            tokens = line.split(maxsplit = 9)
            name = tokens[3]
            suffix = pathlib.Path(name).suffix.replace('.','')
            modified_date = parser.parse(tokens[0]).date()
        
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
    
    @staticmethod
    def dumpFileToFtp(ftp_client, output_directory, output_filename, fileObject):
        content_as_bytes = fileObject

        # cd to directory
        if output_directory:
            ftp_client.cwd(output_directory)

        # Dump to FTP
        return_code = ftp_client.storbinary('STOR ' + output_filename, content_as_bytes)
        return return_code
    
    @staticmethod
    def dumpStringToFTP(ftp_client, output_directory, output_filename, string_content):
        content_as_bytes = BytesIO(bytes(string_content,encoding='utf-8'))

        # cd to directory
        if output_directory:
            ftp_client.cwd(output_directory)

        # Dump to FTP
        return_code = ftp_client.storbinary('STOR ' + output_filename, content_as_bytes)
        return return_code
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
    