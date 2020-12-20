from datetime import datetime, date, time, timedelta
from dateutil import parser
from io import BytesIO
import os
import igc2geojson
import igc_lib
import ftplib
import pathlib
import pytz
import zipfile
import sys

from RunMetadata import RunMetadata
from FtpHelper import FtpHelper
from StorageService import *
from HeatmapFirestoreService import HeatmapFirestoreService
from ServerCredentials import ServerCredentials
from HeatmapBuilder import HeatmapBuilder

# Switch hour for the processing. Before 17 = day -1. After 17 = Current day
SWITCH_HOUR = 17


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
    script_start_time = datetime.datetime.now(tz)

    # ----------------------------- Parse request parameters -----------------------------

    # Heatmap parameters
    target_date = None

    # HACK: This is used to debug localy
    #Request = type('Request', (object,), {})
    #request = Request()
    #request.args = {"targetDate": "2020_05_13"}
    #request.args = {}


    # Parse request parameters
    # ----- Heatmap -----
    if 'targetDate' in request.args:
        target_date = request.args.get('targetDate')
        target_date = datetime.datetime.strptime(target_date, '%Y_%m_%d').date()

    # No target date : Find which date to process
    if target_date is None:
        target_date = date(script_start_time.year,script_start_time.month, script_start_time.day)
        if script_start_time.hour < SWITCH_HOUR:
            target_date = target_date - timedelta(days=1)       # Before 17:00, catchup on previous day

    # Log Start of the process
    processName = "[Heatmap]"
    print(f"##### [Heatmap] Launching processing for: target_date={target_date}")

    # ----------------------------- Begin processing -----------------------------
    global ftp_client_out                       # FTP client to write .geojson outpout

    # Get FTP server credentials from environment variable
    ftp_server_name = os.environ['FTP_SERVER_NAME'].strip()
    ftp_login = os.environ['FTP_LOGIN'].strip()
    ftp_password = os.environ['FTP_PASSWORD'].strip()

    # ---------------------------------------------------- Heatmap ----------------------------------------------
    isUpdateNeeded = False
    return_message = "[HeatmapBuidler] Track up to date. No updated needeed !"
    storageService = StorageService(target_date)
    firestoreService = HeatmapFirestoreService(target_date)

    # --- Run condition ---
    # Find out if the list of files as been modified for the target date. If not, no need to rebuild the track
    print(f"[HeatmapBuidler] Finding out if running is needed for: target_date={target_date}")
    currentHashForDate, currentFilesList = storageService.GetFileListHashForDay()
    lastProcessedHash = firestoreService.GetProcessedFilesHashForDay()
    print(f"New files list / Processed files list: currentHashForDate / lastProcessedHash = {currentHashForDate} / {lastProcessedHash}")


    # Start processing if needed
    if (currentHashForDate != lastProcessedHash):
        print(f"[HeatmapBuidler] Track needs updating ! ...")
        isUpdateNeeded = True and currentFilesList
    else:
        print(return_message)


    if isUpdateNeeded:
        # --- Start the process
        ftp_client_credentials = ServerCredentials(ftp_server_name, ftp_login, ftp_password)
        heatmapBuilder = HeatmapBuilder(ftp_client_credentials, target_date, currentFilesList)
        return_message = heatmapBuilder.metaData.toJSON()

        # Run !
        heatmapBuilder.run()

    # --- Update Firestore progress DB
    firestoreService.UpdateProcessedFilesHasForDay(currentFilesList)                        # Update firestore with hash of processed files

    return  return_message

        


if __name__ == "__main__":
    try:
        res = go(None)
        print(res)
    except SystemExit as e:
        if not e is None:
            print(e)
