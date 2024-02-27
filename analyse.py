import os
import sys
import json
import glob
import shutil
from datetime import datetime
import pandas as pd
from pandas.api.types import is_numeric_dtype
import plotly.express as px

from common import Common

class AnalyseData(Common):

    
    def __init__(self, input_json, loglevel):
        Common.__init__(self)
        jsondata = self.read_inputjson(input_json)
        self.outdir, outbase = self.setup_output_directory(jsondata["output_directory"])
        shutil.copy(input_json, os.path.join(self.outdir, os.path.basename(input_json)))
        self.outfile = os.path.join(self.outdir, outbase + "_" + self.current_datetime)

        self.set_logging(self.outdir, loglevel)
        self.cols_to_print = list([" "])
        self.threshold_cross = jsondata.get("threshold", None)
        self.state_change = jsondata.get("state_change", None)
        self.extra_columns = jsondata.get("extra_columns", list())
        self.create_detailed_csv = jsondata.get("create_detailed_csv", None)
        self.rowsbefore = jsondata.get("rows_before_abnormality", 0)
        self.rowsafter = jsondata.get("rows_after_abnormality", 0)
        self.skiprows = 4
        self.stats = ""
        self.total_toggles = 0
        
        if self.threshold_cross:
            for entry in self.threshold_cross:
                #self.threshold_column_of_interest_list = entry.get("column_of_interest", None)
                #self.threshold = entry.get("value", None)
                self.operator = entry.get("operator", "=")
                 #validate operator- from
                if not any(x != self.operator for x in [">", "<","<=",">=","="]):
                    self.logger.error("Invalid operator")
                    sys.exit(-1)
                #self.logger.info(f"Finding threshold value {self.threshold} crossing in {self.threshold_column_of_interest}")

        if self.state_change:
            self.state_change_column_of_interest = self.state_change.get("column_of_interest", None)
            self.state_change_value1 = self.state_change.get("value_1", None)
            self.state_change_value2 = self.state_change.get("value_2", None)
            self.logger.info(f"Finding state change for column: {self.state_change_column_of_interest}")


    def call_analysis(self, input_json):
        jdata = self.read_inputjson(input_json)
        allcsvs = self.get_files(jdata)
        self.logger.info(allcsvs)

        if allcsvs:
            self.find_abnormalities(allcsvs, jdata)
    
    def check_index(self, dffilter, maindf):
        dfchecked = pd.DataFrame()
        if not dffilter.empty:
            if self.rowsbefore and self.rowsafter:
                st = dffilter.index
                for idx in st:
                    start_idx = max(0, idx - self.rowsbefore)
                    end_idx = min(maindf.shape[0], idx + self.rowsafter)
                    dfchecked = pd.concat([dfchecked, maindf.loc[start_idx:end_idx]], axis=0).drop_duplicates()
            else:
                dfchecked = pd.concat([dfchecked, dffilter], axis=0).drop_duplicates()
                
        return dfchecked

    def check_index_state_change(self, dfs, maindf):
        dftog = pd.DataFrame()
        if not dfs.empty:
            if self.rowsbefore and self.rowsafter:
                st = dfs.index
                for idx in st:
                    start_idx = max(0, idx - self.rowsbefore)
                    end_idx = min(maindf.shape[0], idx + self.rowsafter)
                    dftog = pd.concat([dftog, maindf.loc[start_idx:end_idx]], axis=0).drop_duplicates()
            else:
                dftog = pd.concat([dftog, dfs], axis=0).drop_duplicates()
        return dftog

    def analyse_threshold(self, dft, fname):
        dfall_local = pd.DataFrame()
        exprlist = list()

        if bool(self.threshold_cross):
            for entry in self.threshold_cross:
                if entry["column_of_interest"] not in dft.columns:
                    self.logger.info(f"Threshold column: {entry['column_of_interest']} not found in {fname}")
                    if entry["column_of_interest"] in self.cols_to_print:
                        self.cols_to_print.remove(entry["column_of_interest"])
                else:
                    self.cols_to_print.append(entry["column_of_interest"])
                    # Convert the column to numeric values (ignoring errors)
                    dft[entry["column_of_interest"]] = pd.to_numeric(dft[entry["column_of_interest"]], errors='coerce')
                    # Filter based on the threshold
                    exprlist.append(f"{entry['column_of_interest']} {entry['operator']} {entry['value']}")

            if exprlist:
                expr=" | ".join(x for x in exprlist)
                dfc = dft.query(" & ".join(x for x in exprlist))
            #dfc = dft[dft[ entry["column_of_interest"]] >= 1]

                if dfc.empty:
                    self.logger.info(f"Does not exceed threshold in file {fname}")
                else:
                    # TODO: check if either of rows before or after exist
                    # self.check_index(df, rowsbefore, rowsafter, self.threshold_column_of_interest, cf)
                    self.logger.info(f"Threshold {1} crossed {dfc.shape[0]} times in file {fname} for column {entry['column_of_interest']}")
                    self.stats += f"Threshold {1} crossed {dfc.shape[0]} times in file {fname} for column {entry['column_of_interest']}<br>"

                    dfall_local = self.check_index(dfc, dft)
            return dfall_local



    def analyse_state_change(self, dfs, fname):
        dftoggle = pd.DataFrame()
        if bool(self.state_change):
            if self.state_change_column_of_interest not in dfs.columns:
                self.logger.info(f"{self.state_change_column_of_interest} not found in {fname}")
                if self.state_change_column_of_interest in self.cols_to_print:
                    self.cols_to_print.remove(self.state_change_column_of_interest)
                return dftoggle
            else:
                self.cols_to_print.append(self.state_change_column_of_interest)
                if is_numeric_dtype(dfs[self.state_change_column_of_interest]):
                    self.state_change_value2 = float(self.state_change_value2)
                    self.state_change_value1 = float(self.state_change_value1)
                dfs['shifted'] = dfs[self.state_change_column_of_interest].shift(fill_value=None)
                df1 = dfs[(dfs.shifted == self.state_change_value1) & (dfs[self.state_change_column_of_interest] == self.state_change_value2)]
                df2 = dfs[(dfs.shifted == self.state_change_value2) & (dfs[self.state_change_column_of_interest] == self.state_change_value1)]
                #TODO: check if either of rows before or after exist
                dftoggle1 = self.check_index(df1, dfs)
                dftoggle2 = self.check_index(df2, dfs)
                dftoggle = pd.concat([dftoggle1, dftoggle2], axis=0).drop_duplicates()

                self.logger.info(f"Number of times state change from {self.state_change_value1} to {self.state_change_value2} : {df1.shape[0]}")
                self.logger.info(f"---Number of times state change from {self.state_change_value2} to {self.state_change_value1} : {df2.shape[0]}")
                self.total_toggles += df1.shape[0] + df2.shape[0]
                return dftoggle


    def find_abnormalities(self, cfiles, jsondata):
        
        dfmain = pd.DataFrame()
        dfmain_sc = pd.DataFrame()
        dfall = pd.DataFrame()
        dfall_sc = pd.DataFrame()
        
        for cf in cfiles:
            self.cols_to_print = list(["Local Computer Time"])
            self.logger.debug(f"columns to print {self.cols_to_print}")

            self.logger.info(f"\n----------------Analysing file {cf}--------------------- ")
            try:
                df = pd.read_csv(cf, skiprows=self.skiprows, low_memory=False)
                df.insert(0, "filename", cf)
                self.cols_to_print.append("filename")

            except Exception as e:
                self.logger.error(f"Error parsing file {cf}: {e}")
                continue
            
            if "Local Computer Time" not in df.columns:
                self.logger.error(f"ERROR: Exiting the file because column: Local Computer Time not found columns are not found in {cf}")
                continue
                
            for col in self.extra_columns:
                if col in df.columns:
                    self.cols_to_print.append(col)
                else:
                    self.logger.warning(f"Warning: Could not find extra column {col} in {cf}. Skipping this column")
                    #self.cols_to_print.remove(col)

            dfr = self.analyse_threshold(df, cf)
            if not dfr.empty:
                dfmain = pd.concat([dfmain, dfr[list(self.cols_to_print)]], axis=0).drop_duplicates()
                self.logger.info(f"dfmain: memory usage: {dfmain.memory_usage().sum()/1e6} MB")
                if self.create_detailed_csv:
                    dfall = pd.concat([dfall, dfr], axis=0).drop_duplicates()
                    self.logger.info(f"dfall: memory usage: {dfall.memory_usage().sum()/1e6} MB")

            dfs = self.analyse_state_change(df, cf)
            if not dfs.empty:
                dfmain_sc = pd.concat([dfmain_sc, dfs[list(self.cols_to_print)]], axis=0).drop_duplicates()
                self.logger.info(f"dfmain_sc: memory usage: {dfmain_sc.memory_usage(deep=True).sum()/1e6} MB")
                if self.create_detailed_csv:
                    dfall_sc = pd.concat([dfall_sc, dfs], axis=0).drop_duplicates()
                    self.logger.info(f"dfall_sc: memory usage: {dfall_sc.memory_usage(deep=True).sum()/1e6} MB")
            self.logger.debug(f"columns to print {self.cols_to_print}")
                

        if dfmain.empty and dfmain_sc.empty:
            self.logger.info("No output to process")
            sys.exit(-1)

        if not dfmain.empty:
            self.logger.debug(f"Saving dfmain to {self.outfile} _threshold.csv")
            dfmain.to_csv(self.outfile + "_threshold.csv", index=False)
            self.prepend_extra_lines_csv(4, self.outfile + "_threshold.csv")
            #fig = px.bar(dfmain, y=self.threshold_column_of_interest, x="Local Computer Time")
            self.logger.info(f"Threshold cross Output saved to {self.outfile}_threshold.csv")


        if self.create_detailed_csv:  # Correct handling of create_detailed_csv
            dfall.to_csv(self.outfile + "_threshold_detailed.csv", index=False)
            dfall_sc.to_csv(self.outfile + "_state_toggle_detailed.csv", index=False)
            self.prepend_extra_lines_csv(4, self.outfile + "_threshold_detailed.csv")
            self.logger.info(f"Detailed  Output saved to {self.outfile}_detailed.csv")
            self.prepend_extra_lines_csv(4, self.outfile + "_state_toggle_detailed.csv")
            self.logger.info(f"Detailed  Output saved to {self.outfile}_threshold_detailed.csv and {self.outfile}_state_toggle_detailed.csv")



        if not dfmain_sc.empty:
            dfmain_sc.to_csv(self.outfile + "_state_toggle.csv", index=False)
            self.prepend_extra_lines_csv(4, self.outfile + "_state_toggle.csv")
            self.logger.info(f"State change Output saved to {self.outfile}_state_toggle.csv")

        self.logger.info(f"All results saved in directory: {self.outdir}")
        

    def get_files(self, jsondata):
        input_csvs = jsondata.get("input_csvs", None)
        input_directories = jsondata.get("input_directories", None)
        remove_duplicates = jsondata.get("remove_duplicates", None)
        number_of_days = jsondata.get("number_of_days", None)
        start_date = jsondata.get("start_date", None)
        end_date = jsondata.get("end_date", None)
        end_time = jsondata.get("end_time", None)
        start_time = jsondata.get("start_time", None)
        
        if number_of_days and end_date:
            self.logger.error("""You can use either number_of_days or (start_data and end_date) , not both.
                number_of_days checks for all files from present date back to number of days specified.
                start_date and end_date is used for a range in YYYY-MM-DD format
                If you specify only the start_date, you get to process all files from start_date until today
                default start_time is 00:00
                default end_time is 23.59""")
            sys.exit(-1)

        list_csvs = list()

        if input_csvs:
            for icsv in input_csvs:
                if not os.path.exists(icsv):
                    self.logger.info(f"File not found {icsv}")
                    continue
                list_csvs.append(icsv)

        if input_directories:
            for idir in input_directories:
                if not os.path.exists(idir) or not os.path.isdir(idir):
                    self.logger.info(f"Directory not found {idir} or the path is not a directory")
                    continue

                allcvs = glob.glob( idir + "/**/*.csv", recursive=True )
                list_csvs += allcvs
        
        if not list_csvs:
            self.logger.info("No files found to process")
            return None

        if remove_duplicates:
            self.logger.info("Removing duplicates")
        
            #remove duplicates with same file name even if in different directories:
            #create a map and then discard
            tdict = dict()
            for cf in list_csvs:
                tdict[cf] = os.path.basename(cf)
        
            # Remove duplicate values in dictionary
            # Using loop
            temp = []
            res = dict()
            for key, val in tdict.items():
               if val not in temp:
                   temp.append(val)
                   res[key] = val
        
            list_csvs = list(res.keys())

        toberemoved=list()
        if number_of_days:
            for cf in list_csvs:
                modifiedtstamp = datetime.fromtimestamp(os.path.getmtime(cf))
                ndays = (datetime.now() - modifiedtstamp).days
                if ndays <= number_of_days: 
                    self.logger.info(f"{cf} last modified on {modifiedtstamp} which is within {number_of_days} days. Use this file")
                else:
                    self.logger.info(f"{cf} last modified on {modifiedtstamp} which is older than {number_of_days} days, Discard this file")
                    toberemoved.append(cf)
        
        if start_date:
            stdatetime, etdatetime = self.construct_datatime_from_input(start_time, start_date, end_time, end_date)
            for c in list_csvs:
                modifiedtstamp = datetime.fromtimestamp(os.path.getmtime(c))
                if not stdatetime < modifiedtstamp < etdatetime:
                    self.logger.info(f"{c} last modified on {modifiedtstamp} Not in range. Discard this file")
                    toberemoved.append(c)
                else:
                    self.logger.info(f"{c} last modified on {modifiedtstamp} fits in range. use this file")
        
        for i in toberemoved:
            list_csvs.remove(i)
        
        
        return list_csvs
