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

FTP_HEATMAP_ROOT_DIRECTORY = "heatmap/geojson"
SWITCH_HOUR = 17        # Switch hour for the processing. Before 17 = day -1. After 17 = Current day


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

def get_ftp_client(ftp_server, ftp_username, ftp_password):
    ftp_client = ftplib.FTP(ftp_server, ftp_username, ftp_password)
    return ftp_client

def get_file_from_ftp(ftp_client, filename):
    r = BytesIO()
    ftp_client.retrbinary('RETR ' + filename, r.write)
    return r

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
    is_latest_processing = True

    # Get current time in the right time-zone
    tz = pytz.timezone('Europe/Paris')
    script_start_time = datetime.now(tz)

    # ----------------------------- Parse request parameters -----------------------------
    target_date = None
    relDaysLookup = None
    catchupOnPreviousDay = False
    dry_run = False

    
    ## HACK: This is used to debug localy
    #Request = type('Request', (object,), {})
    #request = Request()
    ##request.args = {"targetDate":"2020_02_09"}
    ##request.args = {"dryRun": True, "targetDate":"2020_02_17", "relDaysLookup":1}
    ##request.args = {"dryRun": True, "relTargetDate":-15}                       # As executed regularly to consolidate map for day-d with flights from day-d until now
    ##request.args = {"dryRun": True, "catchupOnPreviousDay":True}               # As executed between midnight and 17:00 = Will generate map for the previous day with flights from previous + current day
    #request.args = {}                                                           # As executed after 17:00 every day = Will generate map for the day with flights of the day

    # Parse request parameters
    if 'targetDate' in request.args:
        target_date = request.args.get('targetDate')
        target_date = datetime.strptime(target_date, '%Y_%m_%d').date()
        is_latest_processing = False

    if 'relDaysLookup' in request.args:
        relDaysLookup = int(request.args.get('relDaysLookup'))

    if 'relTargetDate' in request.args:
        relTargetDate = int(request.args.get('relTargetDate'))
        now = date(script_start_time.year, script_start_time.month, script_start_time.day)
        target_date = now + timedelta(days=relTargetDate)
        if relDaysLookup is None:
            relDaysLookup = now - target_date
            relDaysLookup = relDaysLookup.days

    if 'catchupOnPreviousDay' in request.args:
        catchupOnPreviousDay = bool(request.args.get('catchupOnPreviousDay'))
        if script_start_time.hour < SWITCH_HOUR:
            target_date = date(script_start_time.year, script_start_time.month, script_start_time.day)
            target_date = target_date - timedelta(days=1)
            relDaysLookup = 1
            is_latest_processing = True


    if 'dryRun' in request.args:
        dry_run = True

    # No target date : Find which date to process
    if target_date is None:
        target_date = date(script_start_time.year, script_start_time.month, script_start_time.day)


    print("##### Launching processing for: target_date={} relDaysLookup={} catchupOnPreviousDay={}".format(target_date, relDaysLookup, catchupOnPreviousDay))
        
    # ----------------------------- Begin processing -----------------------------      
    global ftp_client_igc                       # FTP client to get IGC .zip files
    global ftp_client_out                       # FTP client to write .geojson outpout

    all_files = []      # All files to be parsed (.igc or .zip)
    global_thermals = []
    global_glides = []

    isOutputToGoogleCloudStorage = False

    # Get FTP server credentials from environment variable
    ftp_server_name = os.environ['FTP_SERVER_NAME'].strip()
    ftp_login = os.environ['FTP_LOGIN'].strip()
    ftp_password = os.environ['FTP_PASSWORD'].strip()

    ftp_server_name_igc = os.environ['FTP_SERVER_NAME_IGC'].strip()
    ftp_login_igc = os.environ['FTP_LOGIN_IGC'].strip()
    ftp_password_igc = os.environ['FTP_PASSWORD_IGC'].strip()

    # Create output file name by adding date and time as a suffix
    date_suffix = str(target_date.year) + "_" + str(target_date.month).zfill(2)  + "_" + str(target_date.day).zfill(2)
    output_filename = date_suffix + "-heatmap"
    output_filename_metadata = date_suffix + "-metadata.json"

    output_filename_latest = "latest-heatmap"
    output_filename_metadata_latest = "latest-metadata.json"

    output = "geojson/" + output_filename
    output_latest = "geojson/" + output_filename_latest

    # Init FTP igc client and get file names
    ftp_client_igc = get_ftp_client(ftp_server_name_igc, ftp_login_igc, ftp_password_igc)

    # Get files to process
    all_files = get_file_names_from_ftp(ftp_client_igc, target_date, relDaysLookup)

    ### Analyse files
    files_count = len(all_files)
    flights_count = 0
    flight_date = None
    if all_files:
        for i,filename in enumerate(all_files):
            zip = get_file_from_ftp(ftp_client_igc, filename)

            with zipfile.ZipFile(zip) as zip_file:
                flight = igc_lib.Flight.create_from_zipfile(zip_file)
                if flight.date_timestamp:
                    flight_date = datetime.fromtimestamp(flight.date_timestamp).date()
               
            if flight.valid and flight_date==target_date:
                flights_count += 1
                global_thermals.extend(flight.thermals)
                global_glides.extend(flight.glides)
                print("{}/{} :{} -> {} \t Thermals#={}".format(i+1,files_count, filename, zip_file.filelist[0].filename, len(flight.thermals)))
            else:
                 print("{}/{} :{} -> {} \t Discarded ! valid={} date={}".format(i+1,files_count, filename, zip_file.filelist[0].filename, flight.valid, flight_date))

        # Dump to Google storage
        if isOutputToGoogleCloudStorage:
            # Output to file with date prefix
            igc2geojson.dump_to_google_storage(output, global_thermals)
            print("Google Storage output to: {}".format(output))

            # Output to "latest" filename
            igc2geojson.dump_to_google_storage(output_latest, global_thermals)
            print("Google Storage output to: {}".format(output_latest))

        ### Dump to FTP
        ### HACK: No idea why I can't reuse the same ftp connection to send the same file again with a different name !!
        ## Output to file with date prefix

        if dry_run:
            print("### Dry Run !!!!! ###")
        ## Heatmap
        # Init FTP output client: yyyy_mm_dd-heatmap
        ftp_client_out = get_ftp_client(ftp_server_name, ftp_login, ftp_password)
        if not dry_run:
            igc2geojson.dump_to_ftp(ftp_client_out, FTP_HEATMAP_ROOT_DIRECTORY, output_filename, global_thermals)
        ftp_client_out.close()
        print("GeoJson output to FTP: {} -> {}.geojson".format(ftp_client_out.host, output_filename))   

        # Init FTP output client: latest-heatmap
        if is_latest_processing:
            
            ftp_client_out = get_ftp_client(ftp_server_name, ftp_login, ftp_password)
            if not dry_run:
                igc2geojson.dump_to_ftp(ftp_client_out, FTP_HEATMAP_ROOT_DIRECTORY, output_filename_latest, global_thermals)
            ftp_client_out.close()
            print("GeoJson output to FTP: {} -> {}.json".format(ftp_client_out.host, output_filename_latest)) 

        ## Metadata
        script_end_time = datetime.now(tz)
        metadata = RunMetadata(target_date, script_start_time, script_end_time, flights_count, len(global_thermals))
        json_metadata = metadata.toJSON()
        # Output run metadata information: latest-metadata.json
        if is_latest_processing:
            ftp_client_out = get_ftp_client(ftp_server_name, ftp_login, ftp_password)
            if not dry_run:
                igc2geojson.dump_string_to_ftp(ftp_client_out, FTP_HEATMAP_ROOT_DIRECTORY, output_filename_metadata_latest, json_metadata)
            ftp_client_out.close()
            print("Metadata JSON output to FTP: {} -> {}.json".format(ftp_client_out.host, output_filename_metadata_latest))   

        # Output run metadata information: yyyy_mm_dd-metadata.json
        ftp_client_out = get_ftp_client(ftp_server_name, ftp_login, ftp_password)
        if not dry_run:
            igc2geojson.dump_string_to_ftp(ftp_client_out, FTP_HEATMAP_ROOT_DIRECTORY, output_filename_metadata, json_metadata)
        ftp_client_out.close()
        print("Metadata JSON output to FTP: {} -> {}.json".format(ftp_client_out.host, output_filename_metadata))   

    else:
        print("No .zip file found")
        script_end_time = datetime.now(tz)
        metadata = RunMetadata(target_date, script_start_time, script_end_time, flights_count, len(global_thermals))
        json_metadata = metadata.toJSON()

   
    return_message = json_metadata
    print(return_message)

    # Disconnect FTP
    ftp_client_igc.close()
   
    return return_message

if __name__ == "__main__":
    main(None)
    exit()

