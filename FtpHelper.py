
import ftplib
import pathlib

from io import BytesIO

from datetime import datetime, date, time, timedelta
from dateutil import parser


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