from datetime import datetime, date, time, timedelta
from dateutil import parser
from dateutil.rrule import rrule, DAILY
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
from FirestoreService import FirestoreService
from ServerCredentials import ServerCredentials

# Switch hour for the processing. Before 17 = day -1. After 17 = Current day  ServerCredentials
SWITCH_HOUR = 17
PAST_DAYS_TO_CATCHUP = 15

def main_catchup(request):
    '''
    Catchup on tracemap for the past 15 days
    Re-run the build for each of the past days
    '''
     # Get current time in the right time-zone
    tz = pytz.timezone('Europe/Paris')
    end_date = datetime.now(tz)
    start_date = end_date - timedelta(days=PAST_DAYS_TO_CATCHUP)

    for dt in rrule(DAILY, dtstart=start_date, until=end_date):
        targetDate = dt.strftime("%Y_%m_%d")
        print(f"[CumulativeTrackBuilder] Catching up for targetDate={targetDate}")

        Request = type('Request', (object,), {})
        request = Request()
        request.args = {"targetDate": targetDate}

        main(request)
    

def main_alternative_source(request):
    '''
    This will create the cumulated tracks files from igc files located in a GCP Bucket
    This source bucket is netcoupe-igc-source
    Inside this bucket the name of the "source" where to look for .igc file is the name of the folder
    '''
    if 'source' in request.args:
        sourceFolder = request.args.get('source')

    tz = pytz.timezone('Europe/Paris')
    script_start_time = datetime.now(tz)
    target_date = date(script_start_time.year,script_start_time.month, script_start_time.day)

    cumulativeTrackBuilder = DailyCumulativeTrackBuilder(None, target_date, fileList=None, isOutToLocalFiles=False)
    cumulativeTrackBuilder.run_alternative_source(sourceFolder)



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

    cumulativeTrackBuilder = None

    # ----------------------------- Parse request parameters -----------------------------
    # Cumulative Track parameters
    target_date = None

    # HACK: This is used to debug localy
    #Request = type('Request', (object,), {})
    #request = Request()
    #request.args = {"targetDate": "2020_07_12"}
    #request.args = {}

    # Parse request parameters
    # ----- Cumulative Track -----
    if 'targetDate' in request.args:
        target_date = request.args.get('targetDate')
        target_date = datetime.strptime(target_date, '%Y_%m_%d').date()

    # No target date : Find which date to process
    if target_date is None:
        target_date = date(script_start_time.year,script_start_time.month, script_start_time.day)
        if script_start_time.hour < SWITCH_HOUR:
            target_date = target_date - timedelta(days=1)       # Before 17:00, catchup on previous day

    # Log Start of the process
    print(f"##### Launching processing for: Tracemap target_date={target_date}")

    # ----------------------------- Begin processing -----------------------------
    global ftp_client_out                       # FTP client to write .geojson outpout

    # Get FTP server credentials from environment variable
    ftp_server_name = os.environ['FTP_SERVER_NAME'].strip()
    ftp_login = os.environ['FTP_LOGIN'].strip()
    ftp_password = os.environ['FTP_PASSWORD'].strip()

    # ---------------------------------------------------- Cumulative Track ----------------------------------------------
    isUpdateNeeded = False
    return_message = "[DailyCumulativeTrackBuidler] Track up to date. No updated needeed !"
    storageService = StorageService(target_date)
    firestoreService = FirestoreService(target_date)

    # --- Run condition ---
    # Find out if the list of files as been modified for the target date. If not, no need to rebuild the track
    print(f"[DailyCumulativeTrackBuidler] Finding out if running is needed for: target_date={target_date}")
    currentHashForDate, currentFilesList = storageService.GetFileListHashForDay()
    lastProcessedHash = firestoreService.GetProcessedFilesHashForDay()
    print(f"New files list / Processed files list: currentHashForDate / lastProcessedHash = {currentHashForDate} / {lastProcessedHash}")

    # Start processing if needed
    if (currentHashForDate != lastProcessedHash):
        print(f"[DailyCumulativeTrackBuidler] Track needs updating ! ...")
        isUpdateNeeded = True and currentFilesList
    else:
        print(return_message)

    if (isUpdateNeeded):
        # --- Start the process
        ftp_client_credentials = ServerCredentials(ftp_server_name, ftp_login, ftp_password)
        cumulativeTrackBuilder = DailyCumulativeTrackBuilder(ftp_client_credentials, target_date, fileList=currentFilesList, isOutToLocalFiles=False)

        # Run !
        cumulativeTrackBuilder.run()
        jsonMetadata = cumulativeTrackBuilder.JsonMetaData()

        return_message = jsonMetadata

    # --- Update Firestore progress DB
    firestoreService.UpdateProcessedFilesHasForDay(currentFilesList)                        # Update firestore with hash of processed files
    if (cumulativeTrackBuilder):
        firestoreService.updateFilesStatisticsForDay(cumulativeTrackBuilder.runStatistics)      # Update firestore with statistics

    return return_message


if __name__ == "__main__":
    try:
        res = go(None)
        print(res)
    except SystemExit as e:
        if not e is None:
            print(e)
