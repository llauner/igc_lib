import time
import os
import igc2geojson
import igc_lib

'''
Main entry point for Google function
'''
def main(request):
    isOutputToGoogleCloudStorage = true
    # Create output file name by adding date and time as a suffix
    output = "heatmap"
    
    all_files = []      # All files to be parsed (.igc or .zip)

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



        # Dump to Google storage
        if isOutputToGoogleCloudStorage:
            igc2geojson.dump_to_google_storage(output_filename, global_thermals)
            print("Google Storage output to: {}".format(output))
    else:
        print("No .igc file found")
    return str(time.time())
