import argparse
import pandas as pd
import os
import glob
import sys
from datetime import datetime
from common import Common
import time
import json
import logging 
from analyse import AnalyseData




if __name__ == "__main__":
    args = argparse.ArgumentParser(
        """Script to get certain values above certain threshold in a CSV file"""
    )
    args.add_argument(
        "-j",
        "--input-json",
        required=True,
        help="Path to input json file containing all input arguments"
    )
    args.add_argument(
        "-v",
        "--verbose",
        action = "store_true",
        help="verbose"
    )
    pargs = args.parse_args()
    if not os.path.exists(pargs.input_json):
        print("Input json not found. check path")
        sys.exit(1)
    
    loglevel = logging.INFO
    if pargs.verbose:
        loglevel = logging.DEBUG
    
    analyse = AnalyseData(pargs.input_json, loglevel)
    analyse.call_analysis(pargs.input_json)
 
