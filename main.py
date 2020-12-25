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



if __name__ == "__main__":
    try:
        res = launch_heatmap_catchup(None)
        #res = launch_tracemap(None)
        print(res)
    except SystemExit as e:
        if not e is None:
            print(e)