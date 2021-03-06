#!/usr/bin/env python
from __future__ import print_function

import sys
import igc_lib
import lib.dumpers as dumpers
import geojson as gjson
import zipfile
import numpy as np
import io
import pathlib

from io import BytesIO, StringIO
from datetime import datetime
from datetime import datetime, date, time, timedelta
from google.cloud import storage  # Imports the Google Cloud client library

from HashHelper import HashHelper


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
        vario = round(thermal.vertical_velocity(), 2)
        altitude_enter = int(thermal.enter_fix.press_alt)
        ts = thermal.enter_fix.timestamp

        json_point = gjson.Point((lon, lat, altitude_enter))
        features.append(gjson.Feature(geometry=json_point, properties={"vario": vario,
                                                                       "alt_in": altitude_enter,
                                                                       "ts": ts}))

    feature_collection = gjson.FeatureCollection(features)
    return feature_collection


def get_geojson_track_collection_full(list_flights, isIncludeProperties=True):
    tracks = []
    for flight in list_flights:
        for i in range(0, len(flight.fixes)-2):
            lat1 = flight.fixes[i].lat
            lon1 = flight.fixes[i].lon
            lat2 = flight.fixes[i+1].lat
            lon2 = flight.fixes[i+1].lon

            alt = flight.fixes[i].alt       # Altitude
            ts = flight.fixes[i].timestamp  # Timestamp

            json_line = gjson.LineString([(lon1, lat1), (lon2, lat2)])

            if isIncludeProperties:
                tracks.append(gjson.Feature(geometry=json_line, properties={"alt": alt,
                                                                            "ts": ts}))
            else:
                tracks.append(gjson.Feature(geometry=json_line))

    feature_collection = gjson.FeatureCollection(tracks)
    return feature_collection

# -------- Features ------------------------------------------------------


def get_geojson_feature_track_collection_simple(flight, flightId=None):
    coordinates = []
    for i in range(0, len(flight.fixes)-2):
        lat1 = flight.fixes[i].lat
        lon1 = flight.fixes[i].lon
        lat2 = flight.fixes[i+1].lat
        lon2 = flight.fixes[i+1].lon
        coordinates.append([lon1, lat1])
        coordinates.append([lon2, lat2])

    json_line = gjson.LineString(coordinates)

    # Self computed flightId
    if flightId is None:
        computedFlightIdString = f"{flight.date_timestamp}_{flight.duration}_{flight.pilot_name}_{flight.glider_type}_{flight.glider_id}_{flight.fr_recorder_type}"
        flightId = HashHelper.ComputeHashForList(computedFlightIdString)
        
    # Create feature
    feature = gjson.Feature(geometry=json_line, properties={"flightId": flightId})

    del coordinates

    return feature


def getFeaturesAsFeatureCollection(listFeatures):
    featureCollection = gjson.FeatureCollection(listFeatures)
    return featureCollection


def getJsonFromFeatures(listFeatures):
    featureCollection = getFeaturesAsFeatureCollection(listFeatures)
    json = gjson.dumps(featureCollection)
    return json
# --------------------- Dump to ... -----------------------------------

# --- Features ---


def dumpFeaturesToFile(output_filename, listFeatures):
    feature_collection = getFeaturesAsFeatureCollection(listFeatures)
    dump_feature_collection_to_file(output_filename, feature_collection)


def dump_to_geojson(output_filename, list_thermals):
    # Dump thermals
    feature_collection = get_geojson_feature_collection(list_thermals)

    # Write output: thermals
    with open('{}.geojson'.format(output_filename), 'w') as f:
        gjson.dump(feature_collection, f)


def dump_feature_collection_to_file(output_filename, featureCollection):
    # Write output: Tracks
    with open('{}.geojson'.format(output_filename), 'w') as f:
        gjson.dump(featureCollection, f)

    script_time = datetime.now()
    print("dump_tracks_to_file: end={}".format(script_time))


def dump_tracks_to_file(output_filename, list_flights):
    script_time = datetime.now()
    print("dump_tracks_to_file: start={}".format(script_time))

    # Dump thermals
    feature_collection = get_geojson_track_collection_full(list_flights)

    # Write output: Tracks
    with open('{}.geojson'.format(output_filename), 'w') as f:
        gjson.dump(feature_collection, f)

    script_time = datetime.now()
    print("dump_tracks_to_file: end={}".format(script_time))


def dump_track_to_feature_collection(flight):
    list_flights = [flight]
    # Dump thermals
    feature_collection = get_geojson_track_collection_full(list_flights)

    return feature_collection


def dump_to_google_storage(output_filename, list_thermals):
    # Dump thermals
    feature_collection = get_geojson_feature_collection(list_thermals)
    output_filename = '{}.geojson'.format(output_filename)

    # Instantiates a client
    storage_client = storage.Client()

    bucket = storage_client.get_bucket(google_storage_bucket_id)
    if bucket.exists():
        blob = bucket.blob(output_filename)
        blob.upload_from_string(str(feature_collection))


def dump_to_ftp(ftp_client, output_directory, output_filename, list_thermals):
    # Dump thermals
    feature_collection = get_geojson_feature_collection(list_thermals)
    geojson_file_content = str(feature_collection)

    content_as_bytes = BytesIO(bytes(geojson_file_content, encoding='utf-8'))
    #output_filename = '{}.geojson'.format(output_filename)

    # cd to directory
    if output_directory:
        ftp_client.cwd(output_directory)

    # Dump to FTP
    return_code = ftp_client.storbinary(
        'STOR ' + output_filename, content_as_bytes)
    return return_code


def dump_thermals_to_file(output_filename, list_thermals):
    # Dump thermals
    feature_collection = get_geojson_feature_collection(list_thermals)
    geojson_file_content = str(feature_collection)

    #content_as_bytes = BytesIO(bytes(geojson_file_content,encoding='utf-8'))
    output_filename = '{}.geojson'.format(output_filename)

    # Write output: Tracks
    with open('{}.geojson'.format(output_filename), 'w') as f:
        gjson.dump(feature_collection, f)

    script_time = datetime.now()
    print("dump_tracks_to_file: end={}".format(script_time))


def dump_string_to_ftp(ftp_client, output_directory, output_filename, string_content):
    content_as_bytes = BytesIO(bytes(string_content, encoding='utf-8'))

    # cd to directory
    if output_directory:
        ftp_client.cwd(output_directory)

    # Dump to FTP
    return_code = ftp_client.storbinary(
        'STOR ' + output_filename, content_as_bytes)
    return return_code
