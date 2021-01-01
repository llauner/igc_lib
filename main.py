import main_tracemap

import pandas as pd
from datetime import date, timedelta

import  main_heatmap 
import  main_tracemap



def launch_tracemap(request):
    main_tracemap.main(request)

def launch_heatmap(request):
    main_heatmap.main(request)

def launch_tracemap_catchup(request):
    main_tracemap.main_catchup(request)

def launch_heatmap_catchup(request):
    main_heatmap.main_catchup(request)

def launch_tracemap_alternative_source(request):
    main_tracemap.main_alternative_source(request)




if __name__ == "__main__":
    try:
        Request = type('Request', (object,), {})
        request = Request()
        request.args = {"source": "2020_ludovic"}
        request.args = {"targetDate": "2020_12_30"}

        #res = launch_tracemap_alternative_source(request)
        res = launch_tracemap(request)
        print(res)
    except SystemExit as e:
        if not e is None:
            print(e)