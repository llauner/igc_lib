import main_tracemap

import pandas as pd
from datetime import date, timedelta

import  main_heatmap 
import  main_tracemap


start_date = date(2020, 11, 1)
end_date = date(2020, 11, 30)
delta = timedelta(days=1)

while start_date <= end_date:
    targetDate = start_date.strftime("%Y_%m_%d")
    print(f"Catching up for targetDate={targetDate}")

    Request = type('Request', (object,), {})
    request = Request()
    request.args = {"targetDate": targetDate}

    #main_tracemap.main(request)
    main_heatmap.main(request)

    start_date += delta


