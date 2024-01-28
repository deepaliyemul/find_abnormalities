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
from HTMLHelper import HTMLHelper


class AnalyseData(Common):

    
    def __init__(self, input_json, loglevel):
        Common.__init__(self)
        jsondata = self.read_inputjson(input_json)
        self.outdir, outbase = self.setup_output_directory(jsondata["output_directory"])
        shutil.copy(input_json, os.path.join(self.outdir, os.path.basename(input_json)))
        self.outfile = os.path.join(self.outdir, outbase + "_" + self.current_datetime)

        self.set_logging(self.outdir, loglevel)
        self.cols_to_print = list(["Local Computer Time"])
        self.threshold = jsondata.get("threshold", None)
        self.state_change = jsondata.get("state_change", None)
        self.extra_columns = jsondata.get("extra_columns", list())
        self.create_detailed_csv = jsondata.get("create_detailed_csv", None)
        self.rowsbefore = jsondata.get("rows_before_abnormality", 0)
        self.rowsafter = jsondata.get("rows_after_abnormality", 0)
        self.htmlhelp = HTMLHelper()
        self.skiprows = 4
        self.html = ""
        self.stats = ""
        self.total_toggles = 0
        self.threshold_columns_of_interest = list()
        self.state_change_columns_of_interest = list()


    def build_query_threshold(self, columnlist, jsonlist):
        query = str()
        query_list = list()
        for q in jsonlist:
            if not bool(q.get("column_of_interest")) or not q.get("threshold_value", None): #check if blank or has come data
                continue
            if q["column_of_interest"] not in columnlist:
                continue
            # if above_threshold is not specified, assume default above
            if q["column_of_interest"] not in self.threshold_columns_of_interest:
                self.threshold_columns_of_interest.append(q["column_of_interest"])
            above_threshold = q.get("above_threshold", True)
            if above_threshold:
                query_list.append(f"`{q['column_of_interest']}` >= {q['threshold_value']}")
            else:
                query_list.append(f"`{q['column_of_interest']}` <= {q['threshold_value']}")
        if query_list:
            query =  " | ".join(query_list)
        self.logger.debug(f"Query to search for threshold: {query}")
        return query

    def build_query_state_change(self, columnlist, jsonlist):
        query = str()
        query_list = list()
        for q in jsonlist:
            if not bool(q.get("column_of_interest")) or not "value_1" in q.keys() or not "value_2" in q.keys(): #check if blank or has come data
                self.logger.debug(f"Column of interest, or value_1 or value_2 for state change not found {jsonlist}\
                    bool :{bool(q.get('column_of_interest'))}\
                    bool value_1:{q.get('value_1', None)}\
                    bool value_2:{q.get('value_2', None)}")
                continue
            if q["column_of_interest"] not in columnlist:
                self.logger.debug("Column of interest not in columnlist {columnlist}")
                continue
            if q["column_of_interest"] not in self.state_change_columns_of_interest:
                self.state_change_columns_of_interest.append(q["column_of_interest"])
            val1 = q['value_1'] if not is_numeric_dtype(q["column_of_interest"]) else float(q['value_1'])
            val2 = q['value_2'] if not is_numeric_dtype(q["column_of_interest"]) else float(q['value_2'])
            
            query_list.append(f"(\"{q['column_of_interest']}_shifted\" == {val1} & \"{q['column_of_interest']}\" == {val2})")
            query_list.append(f"(\"{q['column_of_interest']}_shifted\" == {val2} & \"{q['column_of_interest']}\" == {val1})")
        query =  " | ".join(query_list)
        self.logger.debug(f"Query to search for state change :{query}")
        return query

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


    def analyse_threshold(self, dft, fname):
        dfall_local = pd.DataFrame()
        self.threshold_columns_of_interest = list()
        if self.threshold:
            threshold_base_query = self.build_query_threshold(dft.columns, self.threshold)
            # Filter based on the threshold
            self.cols_to_print += self.threshold_columns_of_interest
            if threshold_base_query:
                dfc = dft.query(threshold_base_query)
            else:
                self.logger.error(f"No query formed for file {fname}")
                return dfall_local

            if dfc.empty:
                self.logger.info(f"Does not exceed threshold in file {fname}")
                return dfall_local #return empty dataframe
            else:
                # TODO: check if either of rows before or after exist
                # self.check_index(df, rowsbefore, rowsafter, self.threshold_column_of_interest, cf)
                self.logger.info(f"Threshold criteria met in file {fname} for columns {self.threshold_columns_of_interest}")
                self.stats += f"Threshold criteria met in file {fname} for columns {self.threshold_columns_of_interest}<br>"
                    
                dfall_local = self.check_index(dfc, dft)
                return dfall_local
        else:
            return dfall_local
        #todo test of column exists in one but not in another file



    def analyse_state_change(self, dfs, fname):
        dftoggle = pd.DataFrame()
        self.state_change_columns_of_interest = list()
        if self.state_change:
            state_change_base_query = self.build_query_state_change(dfs.columns, self.state_change)
            if not state_change_base_query:
                self.logger.error(f"No query formed for file {fname}")
                return dftoggle #return emtpy df

            self.cols_to_print += self.state_change_columns_of_interest
            for sccol in self.state_change_columns_of_interest:
                #sccol = entry["column_of_interest"]
                dfs[sccol+'_shifted'] = dfs[sccol].shift(fill_value=None)
                #TODO: check if either of rows before or after exist
            dftog = dfs.query(state_change_base_query)
            dftoggle = self.check_index(dftog, dfs)
            self.logger.info(f"Total state changed in file {fname} : {dftog.shape[0]}")
            self.total_toggles += dftog.shape[0]
            return dftoggle
        else:
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
                if self.create_detailed_csv:
                    dfall = pd.concat([dfall, dfr], axis=0).drop_duplicates()
            
            dfs = self.analyse_state_change(df, cf)
            if not dfs.empty:
                dfmain_sc = pd.concat([dfmain_sc, dfs[list(self.cols_to_print)]], axis=0).drop_duplicates()
                if self.create_detailed_csv:  
                    dfall_sc = pd.concat([dfall_sc, dfs], axis=0).drop_duplicates()
            self.logger.debug(f"columns to print {self.cols_to_print}")
                

        if dfmain.empty and dfmain_sc.empty:
            self.logger.info("No output to process")
            sys.exit(-1)

        self.html = self.htmlhelp.add_page_headers(self.html, "Analysis")
        self.html = self.htmlhelp.browser_compatibility(self.html)

        if not dfmain.empty:
            self.logger.debug(f"Saving dfmain to {self.outfile} _threshold.csv")
            dfmain.to_csv(self.outfile + "_threshold.csv", index=False)
            self.prepend_extra_lines_csv(4, self.outfile + "_threshold.csv")
            #fig = px.bar(dfmain, y=[self.threshold_columns_of_interest], x="Local Computer Time")
            self.html += "<H1>Threshold crossed data</H1>"
            self.html += self.htmlhelp.dataframe_to_html(dfmain)
            #self.html += self.htmlhelp.figure_to_html(fig, title=self.threshold_column_of_interest)
            self.html += self.stats
            self.logger.info(f"Threshold cross Output saved to {self.outfile}_threshold.csv")
            self.logger.debug(f"Threshold df {dfmain.shape}")
                    


        if self.create_detailed_csv:  # Correct handling of create_detailed_csv
            dfall.to_csv(self.outfile + "_threshold_detailed.csv", index=False)
            dfall_sc.to_csv(self.outfile + "_state_toggle_detailed.csv", index=False)
            self.prepend_extra_lines_csv(4, self.outfile + "_threshold_detailed.csv")
            self.logger.info(f"Detailed  Output saved to {self.outfile}_detailed.csv")
            self.prepend_extra_lines_csv(4, self.outfile + "_state_toggle_detailed.csv")
            self.logger.info(f"Detailed  Output saved to {self.outfile}_threshold_detailed.csv and {self.outfile}_state_toggle_detailed.csv")
            self.logger.debug(f"Threshold df detailed {dfall.shape}")
            self.logger.debug(f"State change df detailed {dfall_sc.shape}")




        if not dfmain_sc.empty:
            dfmain_sc.to_csv(self.outfile + "_state_toggle.csv", index=False)
            self.prepend_extra_lines_csv(4, self.outfile + "_state_toggle.csv")
            self.html += f"<H2> Total number of state changes for all files : {self.total_toggles}</H2>"
            self.html += "<H1>State Change data</H1>"
            self.html += self.htmlhelp.dataframe_to_html(dfmain_sc)
            self.logger.info(f"State change Output saved to {self.outfile}_state_toggle.csv")
            self.logger.debug(f"stat_change df detailed {dfmain_sc.shape}")



        if not self.outfile.endswith(".html"):
            self.outfile += ".html"
        self.htmlhelp.write_to_html(self.html,self.outfile)
        self.logger.info(f"HTML Output saved to directory {self.outfile}")
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
