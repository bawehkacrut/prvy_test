import pandas as pd
import numpy as np
import httpagentparser

import logging
from dateutil import parser
from datetime import datetime, date


def get_device_os(ua_str):
    
    res = httpagentparser.simple_detect(str(ua_str))

    return res[0]


def transformDataset(source_path):

    # read dataset
    logging.info ("[Job Info]: Will read dataset")
    df = pd.read_csv(source_path)
    logging.info ("[Job Info]: Read dataset Success!")

    logging.info("[Job Info]: Will trasnform data")
    df['access_type'] = df['userAgent'].str.contains("(MSIE|Trident|(?!Gecko.+)Firefox|(?!AppleWebKit.+Chrome.+)Safari(?!.+Edge)|(?!AppleWebKit.+)Chrome(?!.+Edge)|(?!AppleWebKit.+Chrome.+Safari.+)Edge|AppleWebKit(?!.+Chrome|.+Safari)|Gecko(?!.+Firefox))(?: |\/)([\d\.apre]+)")
    df['access_type'] = np.where(df['access_type'] == True, 'Browser', 'Others')
    df['device_os'] = df['userAgent'].apply(get_device_os)

    logging.info("[Job Info]: Transform data succes!")

    print(f"[Job Info]: Result: \n{df}")
    # df['device_os'] = df['userAgent'].str.extract(r"(iPhone OS \d+_\d+_\d?|Android \d+\d+?)")

