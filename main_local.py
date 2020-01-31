import argparse     as ap
import time
import os
import igc2geojson
import igc_lib

def main():
    log_output = ""
    #Grabs directory and outname
    parser = ap.ArgumentParser()
    parser.add_argument('dir',          help='Path to bulk .igc files'  )
    parser.add_argument('output',       help='Geojson file name'        )
    parser.add_argument('--zip', action='store_true', dest='isZip', help='Get igc file from .zip (1 .igc per .zip)'  )   # Will look for .zip files containing .igc file rather than igc file directly
    parser.add_argument('--out-local-file', action='store_true', dest='isOutputToLocalFile', help='Will write to local file if set to true'  )
    parser.add_argument('--out-google-cloud-storage', action='store_true', dest='isOutputToGoogleCloudStorage', help='Will write to Google Cloud Storage if set to true'  )
    parser.add_argument('--out-suffix-epoch', action='store_true', dest='isOutputWithEpochSuffix', help='Add epoch_ as suffix to output file name'  )
    arguments = parser.parse_args()
    
    dir = arguments.dir
    output = arguments.output
    isZip = arguments.isZip
    isOutputToLocalFile = arguments.isOutputToLocalFile
    isOutputToGoogleCloudStorage = arguments.isOutputToGoogleCloudStorage
    isOutputWithEpochSuffix = arguments.isOutputWithEpochSuffix
    

    # Create output file name by adding date and time as a suffix
    output = arguments.output
    now = epoch_time = int(time.time())
    dir_name = os.path.dirname(output)
    file_name = os.path.basename(output)
    output_filename =  str(now)  + "_" + file_name if isOutputWithEpochSuffix else file_name
    output = dir_name + "\\" if dir_name else "" + output_filename
    
    all_files = []      # All files to be parsed (.igc or .zip)
    
    # Get files
    if not isZip:
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
    global_glides = []

    ### Analyse files
    files_count = len(all_files)
    if all_files:
        for i,file in enumerate(all_files):
            if not isZip:
                flight = igc_lib.Flight.create_from_file("{0}/{1}".format(dir, file))
                print("{}/{} :{} \t Thermals#={}".format(i+1,files_count, file, len(flight.thermals)))
            else:
                zip_filename = "{0}/{1}".format(dir, zip_files[i])
                with zipfile.ZipFile(zip_filename) as zip_file:
                    flight = igc_lib.Flight.create_from_zipfile(zip_file)
                    print("{}/{} :{} -> {} \t Thermals#={}".format(i+1,files_count, file, zip_file.filelist[0].filename, len(flight.thermals)))
        
            if flight.valid:
                global_thermals.extend(flight.thermals)
                global_glides.extend(flight.glides)


        # Dump to GeoJSON
        if isOutputToLocalFile:
            igc2geojson.dump_to_geojson(output, global_thermals)
            print("GeoJson output to: {}".format(output))

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