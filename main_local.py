import argparse     as ap
import time
import os
import igc2geojson
import igc_lib
import ftplib
import pathlib

def get_ftp_client(ftp_server, ftp_username, ftp_password):
    ftp_client = ftplib.FTP(ftp_server, ftp_username, ftp_password)
    return ftp_client

def get_file_names_from_ftp(ftp_client):
    file_names = []

    # Get files list
    files = ftp_client.nlst("igc/2019_igc")

    for f in files:
        suffix = pathlib.Path(f).suffix.replace('.','')
        if suffix == "igc":
            file_names.append(f)

    return file_names

def get_file_as_lines_from_ftp(ftp_client, filename):
    file_lines = []

    ftp_client.retrlines('RETR %s' % filename, file_lines.append)
    flight = igc_lib.Flight.create_from_lines(file_lines)
    
    return file_lines

def main():
    global ftp_client
    log_output = ""

    #Grabs directory and outname
    parser = ap.ArgumentParser()
    parser.add_argument('dir',          help='Path to bulk .igc files'  )
    parser.add_argument('output',       help='Geojson file name'        )
    parser.add_argument('--zip', action='store_true', dest='isZip', help='Get igc file from .zip (1 .igc per .zip)'  )   # Will look for .zip files containing .igc file rather than igc file directly
    parser.add_argument('--out-local-file', action='store_true', dest='isOutputToLocalFile', help='Will write to local file if set to true'  )
    parser.add_argument('--out-google-cloud-storage', action='store_true', dest='isOutputToGoogleCloudStorage', help='Will write to Google Cloud Storage if set to true'  )
    parser.add_argument('--out-suffix-epoch', action='store_true', dest='isOutputWithEpochSuffix', help='Add epoch_ as suffix to output file name'  )
    parser.add_argument('--ftp', nargs=3, help='Get the igc files from FTP <server name> <username> <password>'  )
    parser.add_argument('--ftp-from-env', action='store_true', dest='isFtpFromEnvironmentVariable', help='Will get FTP details from environment variables'  )
    parser.add_argument('--out-ftp', action='store_true', dest='isOutputToFtp', help='Will write output to FTP'  )
    parser.add_argument('--dump-track', action='store_true', dest='isDumpTrack', help='Will dump tracks if set to true'  )
    arguments = parser.parse_args()
    
    dir = arguments.dir
    output = arguments.output
    isZip = arguments.isZip
    isOutputToLocalFile = arguments.isOutputToLocalFile
    isOutputToGoogleCloudStorage = arguments.isOutputToGoogleCloudStorage
    isOutputWithEpochSuffix = arguments.isOutputWithEpochSuffix
    isFtpFromEnvironmentVariable = arguments.isFtpFromEnvironmentVariable
    isFtp = False
    ftp_server = None
    ftp_username = None
    ftp_password = None
    isOutputToFtp = arguments.isOutputToFtp

    if arguments.ftp:
        isFtp = True
        ftp_server = arguments.ftp[0]
        ftp_username = arguments.ftp[1]
        ftp_password = arguments.ftp[2]
    elif isFtpFromEnvironmentVariable:
        isFtp = True
        ftp_server = os.environ['FTP_SERVER_NAME'].strip()
        ftp_username = os.environ['FTP_LOGIN'].strip()
        ftp_password = os.environ['FTP_PASSWORD'].strip()

    isDumpTrack = arguments.isDumpTrack
    

    # Create output file name by adding date and time as a suffix
    output = arguments.output
    now = epoch_time = int(time.time())
    dir_name = os.path.dirname(output)
    file_name = os.path.basename(output)
    output_filename =  str(now)  + "_" + file_name if isOutputWithEpochSuffix else file_name
    output = dir_name + ("\\" if dir_name else "") + output_filename
    
    all_files = []      # All files to be parsed (.igc or .zip)
    
    # Get files
    if isFtp:
        ftp_client = get_ftp_client(ftp_server, ftp_username, ftp_password)
        all_files = get_file_names_from_ftp(ftp_client)
    elif not isZip:
    # Read .igc files names in a directory
        igc_files = []
        for file in os.listdir("{}".format(dir)):
            if file.endswith(".igc"):
                igc_files.append(file)
        all_files = igc_files
    else:
        # Read .zip files names in a directory
        zip_files = []
        if not zip_files:
            for file in os.listdir("{}".format(dir)):
                if file.endswith(".zip"):
                    zip_files.append(file)
            all_files = zip_files


    ### Collect all flights
    global_thermals = []
    global_flights = []

    ### Analyse files
    files_count = len(all_files)
    if all_files:
        for i,file in enumerate(all_files):
            if isFtp:
                ftp_file_lines = get_file_as_lines_from_ftp(ftp_client, file)
                flight = igc_lib.Flight.create_from_lines(ftp_file_lines)
                print("{}/{} :{} \t Thermals#={}".format(i+1,files_count, file, len(flight.thermals)))

            elif not isZip:
                flight = igc_lib.Flight.create_from_file("{0}/{1}".format(dir, file))
                print("{}/{} :{} \t Thermals#={}".format(i+1,files_count, file, len(flight.thermals)))
            else:
                zip_filename = "{0}/{1}".format(dir, zip_files[i])
                with zipfile.ZipFile(zip_filename) as zip_file:
                    flight = igc_lib.Flight.create_from_zipfile(zip_file)
                    print("{}/{} :{} -> {} \t Thermals#={}".format(i+1,files_count, file, zip_file.filelist[0].filename, len(flight.thermals)))
        
            if flight.valid:
                global_thermals.extend(flight.thermals)
                global_flights.append(flight)


        ### Dump to GeoJSON
        # Dump to local file
        if isOutputToLocalFile:
            igc2geojson.dump_to_geojson(output, global_thermals)
            print("GeoJson output to: {}".format(output))

            # Dump Tracks
            if isDumpTrack:
                tracks_filename = output + '_tracks'
                igc2geojson.dump_tracks_to_file(tracks_filename, global_flights)
                print("GeoJson output to: {}".format(tracks_filename))


        # Dump to FTP
        elif isOutputToFtp:
            igc2geojson.dump_to_ftp(ftp_client,output, global_thermals)
            print("GeoJson output to FTP: {}".format(ftp_client))

        # Dump to Google storage
        if isOutputToGoogleCloudStorage:
            igc2geojson.dump_to_google_storage(output_filename, global_thermals)
            print("Google Storage output to: {}".format(output))
    else:
        print("No .igc file found")
    return str(time.time())

if __name__ == "__main__":
    main()
    exit()