import argparse
import pandas as pd
import os
import glob
import sys
from datetime import datetime
import plotly.express as px
import time
import json
from pandas.api.types import is_numeric_dtype

current_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M")
runlog = open('run_log' + current_datetime + ".log", 'w')


def printing(text):
    print(text)
    if runlog:
        runlog.write(str(text) + "\n")

 
class HTMLHelper(object):
    def browser_compatibility(self, text):
        htmlprepend = f"""
        <html>
        <head>
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-1BmE4kWBq78iYhFldvKuhfTAU6auU8tT94WrHftjDbrCEXSU1oBoqyl2QvZ6jIW3" cross
origin="anonymous">
            <meta charset="utf-8" />
        </head>
        <body>
            <div>
                <script type="text/javascript">window.PlotlyConfig = {{MathJaxConfig: \'local\'}};</script>
      
                <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            </div>
          {text}
        </body>
        </html>
        """
        return htmlprepend

    def add_page_headers(self, text, pheader):
        html = f"""<H1>{pheader}<br></H1><p style="font-size:11px">Last updated: {datetime.now().strftime("%Y-%m-%d %H:%M")}</p>"""
        return html + text

    def write_to_html(self, text, outfile):
        html_full = text
        if not outfile.endswith(".html"):
            outfile += ".html"
        # create directory is non existent
        if os.path.dirname(outfile):
            os.makedirs(os.path.dirname(outfile), exist_ok=True)
        with open(outfile, "w") as f:
            f.write(html_full)
            printing(f"Output file at: {outfile}")

    def dataframe_to_html(self, df, input_index=False, title=None, csv=False):
        html = """
            <style>
            table, th, td {
              border: 1px solid black;
              border-collapse: collapse;
              font-size: 14px;
              white-space:nowrap;
              text-align: left;
            }
            </style>
            """
        if title:
            html += "<header><h1>" + title + "</h1></header>"
        html += df.to_html(index=input_index)
        return html

    def figure_to_html(
        self,
        fig,
        title,
        height=800,
        width=1920,
        xaxis_title="Date and Time",
        yaxis_title="Value",
        ):
        fig.update_layout(
            xaxis_title=xaxis_title,
            yaxis_title=yaxis_title,
        )
        fig.layout.height = height
        fig.layout.width = width
        fig.layout.title = title
        html = f"""
                {fig.to_html(config=dict(displaylogo=False),include_plotlyjs=False, full_html=False)}
        """
        return html


###############
def find_abnormalities(cfiles, threshold_cross, state_change, outdir, extra_columns, create_detailed_csv, rowsbefore, rowsafter):
    htmlhelp = HTMLHelper()
    html = ""
    stats = ""
    dfmain = pd.DataFrame()
    dfall = pd.DataFrame()
    dftoggle = pd.DataFrame()
    toggle_total = 0

    outdirnew = os.path.join(os.path.dirname(outdir), current_datetime + "_" + os.path.basename(outdir))

    if not os.path.exists(outdirnew):
        print(f"Creating directory {outdirnew}")
        os.makedirs(outdirnew)

    outfile = os.path.join(outdirnew, current_datetime + "_" + os.path.basename(outdir))

    if threshold_cross:
        threshold_column_of_interest = threshold_cross.get("column_of_interest", None)
        threshold = threshold_cross.get("threshold_value", None)
        printing(f"Finding threshold value {threshold} crossing in {threshold_column_of_interest}")

    if state_change:
        state_change_column_of_interest = state_change.get("column_of_interest", None)
        state_change_value1 = state_change.get("value_1", None)
        state_change_value2 = state_change.get("value_2", None)
        printing(f"Finding state change for column: {state_change_column_of_interest}")

    for cf in cfiles:
        print(f"\n----------------Analysing file {cf}--------------------- ")
        df = pd.read_csv(cf, skiprows=4, low_memory=False)
        df.insert(0, "filename", cf)
        if "Local Computer Time" not in df.columns:
            print(f"ERROR: Exiting the file because the Local computer time not found columns are not found in {cf}")
            continue

        cols_to_print = ["Local Computer Time", "filename"]
            
        for col in extra_columns:
            if col in df.columns:
                cols_to_print.append(col)
            else:
                print(f"Warning: Could not find extra column {col} in {cf}. Skipping this column")

        if threshold_cross:
            if threshold_column_of_interest not in df.columns:
                print(f"{threshold_column_of_interest} not found in {cf}")
            else:
                cols_to_print.append(threshold_column_of_interest)
                dfc = df[df[threshold_column_of_interest] >= threshold]
                if dfc.empty:
                    print(f"Does not exceed threshold in file {cf}")
                else:
                    #TODO: check if either of rows before or after exist
                    if rowsbefore and rowsafter:
                        crossing_indices = dfc.index
                        print(len(crossing_indices))
                        for idx in crossing_indices:
                            start_idx = max(0, idx - rowsbefore)
                            end_idx = min(df.shape[0], idx + rowsafter)

                            #print(f"\nThreshold {threshold} crossed in file {cf} for column {threshold_column_of_interest}")

                            dfc1 = df.loc[start_idx:end_idx, cols_to_print]
                            dfmain = pd.concat([dfmain, dfc1[cols_to_print]], axis=0).drop_duplicates()
                            dfall = pd.concat([dfall, df.loc[start_idx: end_idx]], axis=0).drop_duplicates()
                    else:
                        dfmain = pd.concat([dfmain, dfc[cols_to_print]], axis=0).drop_duplicates()
                        dfall = pd.concat([dfall, dfc], axis=0).drop_duplicates()
                        
                    stats += f"Threshold {threshold} crossed {dfc.shape[0]} times in file {cf} for column {threshold_column_of_interest}<br>"

        if state_change:
          
            if state_change_column_of_interest not in df.columns:
                printing(f"{state_change_column_of_interest} not found in {cf}")
            else:
                cols_to_print.append(state_change_column_of_interest)
                if is_numeric_dtype(df[state_change_column_of_interest]):
                    state_change_value2 = float(state_change_value2)
                    state_change_value1 = float(state_change_value1)
                df['shifted'] = df[state_change_column_of_interest].shift(fill_value=0)
                df1 = df[(df.shifted == state_change_value1) & (df[state_change_column_of_interest] == state_change_value2)]
                df2 = df[(df.shifted == state_change_value2) & (df[state_change_column_of_interest] == state_change_value1)]
                #TODO: check if either of rows before or after exist
                if not df1.empty:
                    if rowsbefore and rowsafter:
                        st_1 = df1.index
                        for idx in st_1:
                            start_idx_1 = max(0, idx - rowsbefore)
                            end_idx_1 = min(df.shape[0], idx + rowsafter)
                            dftoggle = pd.concat([dftoggle, df.loc[start_idx_1:end_idx_1][cols_to_print]], axis=0).drop_duplicates()
                    else:
                        dftoggle = pd.concat([dftoggle, df1[cols_to_print]], axis=0).drop_duplicates()

                if not df2.empty :
                    if rowsbefore and rowsafter:
                        st_2 = df2.index
                        for idx in st_2:
                            start_idx_2 = max(0, idx - rowsbefore)
                            end_idx_2 = min(df.shape[0], idx + rowsafter)
                            dftoggle = pd.concat([dftoggle, df.loc[start_idx_2:end_idx_2][cols_to_print]], axis=0).drop_duplicates()
                    else:
                        dftoggle = pd.concat([dftoggle, df2[cols_to_print]], axis=0).drop_duplicates()


                printing(f"Number of times state change from {state_change_value1} to {state_change_value2} : {df1.shape[0]}")
                printing(f"---Number of times state change from {state_change_value2} to {state_change_value1} : {df2.shape[0]}")
                toggle_total += df1.shape[0] + df2.shape[0]
                printing(f"Total number of toggles : {toggle_total}")

    if dfmain.empty and dftoggle.empty:
        printing("No output to process")
        sys.exit(-1)

    html = htmlhelp.add_page_headers(html, "Analysis")
    html = htmlhelp.browser_compatibility(html)

    if not dfmain.empty:
        dfmain.to_csv(outfile + "_threshold.csv", index=False)
        insert_extra_lines(4, outfile + "_threshold.csv")
        fig = px.bar(dfmain, y=threshold_column_of_interest, x="Local Computer Time")
        html += "<H1>Threshold crossed data</H1>"
        html += htmlhelp.dataframe_to_html(dfmain)
        html += htmlhelp.figure_to_html(fig, title=threshold_column_of_interest)
        html += stats

    if create_detailed_csv:  # Correct handling of create_detailed_csv
        dfall.to_csv(outfile + "_detailed.csv", index=False)
        insert_extra_lines(4, outfile + "_detailed.csv")

    if not dftoggle.empty:
        dftoggle.to_csv(outfile + "_state_toggle.csv", index=False)
        insert_extra_lines(4, outfile + "_state_toggle.csv")
        html += f"<H2> Total number of state changes for all files : {toggle_total}</H2>"
        html += "<H1>State Change data</H1>"
        html += htmlhelp.dataframe_to_html(dftoggle)

    if not outfile.endswith(".html"):
        outfile += ".html"
    htmlhelp.write_to_html(html, outfile)

##################


def insert_extra_lines(number_of_extra_lines, fname):
    with open(fname, 'r+') as file:
            readcontent = file.read()  # store the read value of exe.txt into 
                                # readcontent 
            file.seek(0, 0) #Takes the cursor to top line
            file.write(number_of_extra_lines*"\n") #convert int to str since write() deals 
                               # with str
            file.write(readcontent) #after content of string are written, I return 
                             #back content that were in the file
            printing(f"Output saved as csv file at: {fname}")



def print_columns(csvfile):       
    dfr = pd.read_csv(csvfile, skiprows=4,  low_memory=False)
    with open("column_list.txt", "w") as f:
        for c in dfr.columns:
            f.write(c + "\n")
    printing("List of all column names written to file: column_list.txt")
    


def construct_datatime_from_input(start_time, start_date, end_time, end_date):
    if not start_time:
        start_time = "00:00"
    if not end_date:
        et = datetime.now()
    if end_date:
        if not end_time:
            end_time = "23:59"
        et = datetime.strptime(end_date + " "+ end_time, '%Y-%m-%d %H:%M')

    #construct datetime from the string
    st = datetime.strptime(start_date + " " + start_time, '%Y-%m-%d %H:%M')
    
    printing("Start date time", st)
    printing("End date time", et)
    return st, et



def file_within_date_time_range(cfile, st, et):
    modifiedtstamp = datetime.fromtimestamp(os.path.getmtime(cfile))

    if st < modifiedtstamp < et:
        printing(f"This file is to be included : {cfile}")

    else:
        printing(f"discard")



def get_files(input_csvs, input_directories, number_of_days, start_time=None, start_date=None, end_time=None, end_date=None, remove_duplicates=False):
    list_csvs = list()

    if input_csvs:
        for icsv in input_csvs:
            if not os.path.exists(icsv):
                printing(f"File not found {icsv}")
                continue
            list_csvs.append(icsv)

    if input_directories:
        for idir in input_directories:
            if not os.path.exists(idir) or not os.path.isdir(idir):
                printing(f"Directory not found {idir} or the path is not a directory")
                continue

            allcvs = glob.glob( idir + "/**/*.csv", recursive=True )
            list_csvs += allcvs
    
    if not list_csvs:
        printing("No files found to process")
        sys.exit(-1)

    if remove_duplicates:
        printing("Removing duplicates")
    
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
                printing(f"{cf} last modified on {modifiedtstamp} which is within {number_of_days} days. Use this file")
            else:
                printing(f"{cf} last modified on {modifiedtstamp} which is older than {number_of_days} days, Discard this file")
                toberemoved.append(cf)
    
    if start_date:
        stdatetime, etdatetime = construct_datatime_from_input(start_time, start_date, end_time, end_date)
        for c in list_csvs:
            modifiedtstamp = datetime.fromtimestamp(os.path.getmtime(c))
            if not stdatetime < modifiedtstamp < etdatetime:
                printing(f"{c} last modified on {modifiedtstamp} Not in range. Discard this file")
                toberemoved.append(c)
            else:
                printing(f"{c} last modified on {modifiedtstamp} fits in range. use this file")
    
    for i in toberemoved:
        list_csvs.remove(i)
    
    
    return list_csvs


def get_args_from_input_json(jfile):
    jargs = None

    with open(jfile, 'r') as f:
        readraw = f.readlines()
    with open("tmpfile", 'w') as fw:    
        for line in readraw:
            if '\\' in line:
                printing(line)
                fw.write(line.replace("\\", "\\\\"))
            else:
                fw.write(line)

    with open('tmpfile', 'r') as fr:
        jdata = json.load(fr)
    
    os.remove("tmpfile")
    if not jdata.get("threshold_cross", None) and not jdata.get("state_change", None):
        printing("Please add either threshold_cross or state_change in json file")
        return


    list_of_files = get_files(
        jdata.get('input_csvs', None),
        jdata.get('input_directories', None),
        jdata.get('number_of_days', None),
        jdata.get('start_time', None),
        jdata.get('start_date', None),
        jdata.get('end_time', None),
        jdata.get('end_date', None)
        )
    
    find_abnormalities(list_of_files,
        jdata.get('threshold_cross',None),
        jdata.get('state_change', None),
        jdata.get('output_directory', 'testdir'),
        jdata.get('extra_columns', None),
        jdata.get('create_detailed_csv', None),
        jdata.get('rows_before_abnormality', None),
        jdata.get('rows_after_abnormality', None)
        )



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
    
    pargs = args.parse_args()
    
    #ignore everything else, use only the json
    if not os.path.exists(pargs.input_json):
        printing("Input json not found. check path")
        sys.exit(1)
    get_args_from_input_json(pargs.input_json)
    runlog.close()