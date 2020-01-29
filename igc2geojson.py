#!/usr/bin/env python
from __future__ import print_function

import os
import sys
import igc_lib
import lib.dumpers as dumpers
import argparse     as ap
import time
import geojson as gjson
import zipfile
import numpy as np


def print_flight_details(flight):
    print("Flight:", flight)
    print("Takeoff:", flight.takeoff_fix)
    thermals = flight.thermals

    for i in range(len(thermals)):
        thermal = thermals[i]
        print("  thermal[%d]:" % i, thermals[i])
    print("Landing:", flight.landing_fix)


def dump_flight(flight, input_file):
    input_base_file = os.path.splitext(input_file)[0]
    wpt_file = "%s-thermals.wpt" % input_base_file
    cup_file = "%s-thermals.cup" % input_base_file
    thermals_csv_file = "%s-thermals.csv" % input_base_file
    flight_csv_file = "%s-flight.csv" % input_base_file
    kml_file = "%s-flight.kml" % input_base_file

    print("Dumping thermals to %s, %s and %s" %
          (wpt_file, cup_file, thermals_csv_file))
    dumpers.dump_thermals_to_wpt_file(flight, wpt_file, True)
    dumpers.dump_thermals_to_cup_file(flight, cup_file)

    print("Dumping flight to %s and %s" % (kml_file, flight_csv_file))
    dumpers.dump_flight_to_csv(flight, flight_csv_file, thermals_csv_file)
    dumpers.dump_flight_to_kml(flight, kml_file)

   
def dump_to_geojson(output_filename, list_thermals, list_glides):
    """
    
    """
    ### Dump thermals ###
    features = []

    for thermal in list_thermals:
        #print(thermal)
        lat = thermal.enter_fix.lat
        lon = thermal.enter_fix.lon
        vario = round(thermal.vertical_velocity(),2)
        altitude_enter = int(thermal.enter_fix.press_alt)

        json_point=gjson.Point((lon, lat, altitude_enter))
        features.append(gjson.Feature(geometry=json_point, properties={"vario": vario, 
                                                                       "alt_in": altitude_enter}))

    feature_collection = gjson.FeatureCollection(features)
    #Write output
    with open('{}.geojson'.format(output_filename), 'w') as f:
        gjson.dump(feature_collection, f)

    #### Dump Glides ###
    ## Keep glides with sink rate <=2m/s
    #glide_features = []
    #for glide in list_glides:
    #    lat_enter, lon_enter = glide.enter_fix.lat, glide.enter_fix.lon
    #    lat_exit, lon_exit = glide.exit_fix.lat, glide.exit_fix.lon
    #    altitude_enter = int(glide.enter_fix.press_alt)
    #    altitude_exit = int(glide.exit_fix.press_alt)
    #    time_enter = glide.enter_fix.rawtime
    #    time_exit = glide.exit_fix.rawtime
    #    vario = glide.average_vario()
        
    #    if vario <= -1.5:
    #        json_line=gjson.LineString([(lon_enter, lat_enter, altitude_enter),(lon_exit, lat_exit, altitude_exit)])
    #        glide_features.append(gjson.Feature(geometry=json_line, properties={"vario": vario, 
    #                                                                            "time_in": time_enter, 
    #                                                                            "time_out": time_exit, 
    #                                                                            "alt_in": altitude_enter, 
    #                                                                            "alt_out": altitude_exit}))

    #feature_collection = gjson.FeatureCollection(glide_features)
    ##Write output
    #with open('{}_coldMap.geojson'.format(output_filename), 'w') as f:
    #    gjson.dump(feature_collection, f)


def main():
    #Grabs directory and outname
    parser = ap.ArgumentParser()
    parser.add_argument('dir',          help='Path to bulk .igc files'  )
    parser.add_argument('output',       help='Geojson file name'        )
    parser.add_argument('--zip', action='store_true', dest='isZip', help='Get igc file from .zip (1 .igc per .zip)'  )   # Will look for .zip files containing .igc file rather than igc file directly
    arguments = parser.parse_args()
    
    dir = arguments.dir
    output = arguments.output
    isZip = arguments.isZip

    # Create output file name by adding date and time as a suffix
    output = arguments.output
    now = epoch_time = int(time.time())
    dir_name = os.path.dirname(output)
    file_name = os.path.basename(output)
    output = dir_name + "\\" + str(now) + "_" + file_name
    
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
        dump_to_geojson(output, global_thermals, global_glides)
        print("GeoJson output to: {}".format(output))
    else:
        print("No .igc file found")


if __name__ == "__main__":
    main()
    exit()


