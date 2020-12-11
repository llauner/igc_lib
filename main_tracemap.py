from datetime import datetime, date, time, timedelta
from dateutil import parser
from io import BytesIO
import os
import igc2geojson
import igc_lib
import ftplib
import pathlib
import pytz
import sys

from RunMetadata import RunMetadata
from DailyCumulativeTrackBuilder import *
from FtpHelper import FtpHelper


'''
Main entry point for Google function
'''


def main(request):
    '''
    Args:
        day=yyyy_mm_dd: The yyyy_mm_dd for which to do the processing
        catchUpOnDay=d: Launch the processing for day = day-d
            This is used to consolidate the processing on day d+1 with files that were submitted after day d.
            Example: When executed with -1 on Tuesday this will get the flights from Monday, and also the fligths from Tuesday and look up
            inside the IGC to see if the flight was done on monday. Will then take it into account
    '''
    # Get current time in the right time-zone
    tz = pytz.timezone('Europe/Paris')
    script_start_time = datetime.now(tz)

    # ----------------------------- Parse request parameters -----------------------------
    # Cumulative Track parameters
    target_date = None
    cumulativeTrackBuilder = None

    # HACK: This is used to debug localy
    Request = type('Request', (object,), {})
    request = Request()
    request.args = {"targetDate": "2020_07_07"}
    #request.args = {}

    # Parse request parameters
    # ----- Cumulative Track -----
    if 'targetDate' in request.args:
        target_date = request.args.get('targetDate')
        target_date = datetime.strptime(target_date, '%Y_%m_%d').date()

    # No target date : Find which date to process
    if target_date is None:
        target_date = date(script_start_time.year,
                           script_start_time.month, script_start_time.day)

    # Log Start of the process
    print(f"##### Launching processing for: Tracemap target_date={target_date}")

    # ----------------------------- Begin processing -----------------------------
    global ftp_client_out                       # FTP client to write .geojson outpout

    # Get FTP server credentials from environment variable
    ftp_server_name = os.environ['FTP_SERVER_NAME'].strip()
    ftp_login = os.environ['FTP_LOGIN'].strip()
    ftp_password = os.environ['FTP_PASSWORD'].strip()

    # Create output file name by adding date and time as a suffix
    date_suffix = str(target_date.year) + "_" + str(target_date.month).zfill(2) + "_" + str(target_date.day).zfill(2)
    output_filename = date_suffix + "-heatmap"
    output_filename_metadata = date_suffix + "-metadata.json"

    output_filename_latest = "latest-tracemap"
    output_filename_metadata_latest = "latest-metadata.json"

    output = "geojson/" + output_filename
    output_latest = "geojson/" + output_filename_latest

    # ---------------------------------------------------- Cumulative Track ----------------------------------------------
    # --- Start the process
    ftp_client_out = FtpHelper.get_ftp_client(ftp_server_name, ftp_login, ftp_password)

    cumulativeTrackBuilder = DailyCumulativeTrackBuilder(ftp_client_out, target_date, isOutToLocalFiles=True)

    # Run !
    cumulativeTrackBuilder.run()
    jsonMetadata = cumulativeTrackBuilder.JsonMetaData()

    return_message = jsonMetadata

    # Disconnect FTP
    ftp_client_out.close()

    return return_message


if __name__ == "__main__":
    try:
        res = main(None)
        print(res)
    except SystemExit as e:
        if not e is None:
            print(e)
