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
from google.cloud import storage # Imports the Google Cloud client library


google_storage_bucket_id = "bucket_heatmap"      # Google storage bucket name


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

def get_geojson_feature_collection(list_thermals):
     ### Dump thermals into geojson ###
    features = []

    for thermal in list_thermals:
        lat = thermal.enter_fix.lat
        lon = thermal.enter_fix.lon
        vario = round(thermal.vertical_velocity(),2)
        altitude_enter = int(thermal.enter_fix.press_alt)

        json_point=gjson.Point((lon, lat, altitude_enter))
        features.append(gjson.Feature(geometry=json_point, properties={"vario": vario, 
                                                                       "alt_in": altitude_enter}))

    feature_collection = gjson.FeatureCollection(features)
    return feature_collection

   
def dump_to_geojson(output_filename, list_thermals):
    # Dump thermals
    feature_collection = get_geojson_feature_collection(list_thermals)

    #Write output
    with open('{}.geojson'.format(output_filename), 'w') as f:
        gjson.dump(feature_collection, f)

def test_google_storage():
    # Instantiates a client
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(google_storage_bucket_id)
    if bucket.exists():
        blob = bucket.blob("delete_me.txt")

        blob.upload_from_string("hello !!")


def dump_to_google_storage(output_filename,list_thermals):
    # Dump thermals
    feature_collection = get_geojson_feature_collection(list_thermals)
    output_filename = '{}.geojson'.format(output_filename)

    # Instantiates a client
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(google_storage_bucket_id)
    if bucket.exists():
        blob = bucket.blob(output_filename)
        blob.upload_from_string(str(feature_collection))

def main():
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
            dump_to_geojson(output, global_thermals)
            print("GeoJson output to: {}".format(output))

        # Dump to Google storage
        if isOutputToGoogleCloudStorage:
            dump_to_google_storage(output_filename, global_thermals)
            print("Google Storage output to: {}".format(output))
    else:
        print("No .igc file found")


if __name__ == "__main__":
    main()
    exit()


