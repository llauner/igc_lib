#!/usr/bin/env python
from __future__ import print_function

import sys
import igc_lib
import lib.dumpers as dumpers
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

