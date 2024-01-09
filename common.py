import logging
from datetime import datetime
import os
import json

class Common(object):

    def __init__(self):
        self.logger = logging.getLogger('common')
        self.split_chunks = 2
        self.current_datetime = datetime.now().strftime("%Y-%m-%d-%H-%M")
        
    def read_inputjson(self, jfile):
        jdata = None
        
        with open(jfile, 'r') as f:
            readraw = f.readlines()
        with open("tmpfile", 'w') as fw:    
            for line in readraw:
                if '\\' in line:
                    fw.write(line.replace("\\", "\\\\"))
                else:
                    fw.write(line)

        with open('tmpfile', 'r') as fr:
            jdata = json.load(fr)
        
        os.remove("tmpfile")
        return jdata


    def setup_output_directory(self, output_dir):
        outbase = os.path.basename(output_dir)
        outdir = os.path.dirname(output_dir)
        if not bool(outdir):
        	outdir = os.getcwd()
        if not bool(outbase):
            outbase = "test"
        
        outdirnew = os.path.join(outdir, outbase + "_" + self.current_datetime)
        if not os.path.exists(outdirnew):
            self.logger.info(f"Creating new directory:{outdir}")
            os.makedirs(outdirnew)
        return outdirnew, outbase


    def set_logging(self, logdir, level=logging.INFO):
        logfile = os.path.join(logdir, "log_" + format(self.current_datetime))
        logging.basicConfig(
                        level=level,
                        format="%(asctime)s [%(levelname)s] %(message)s",
                        handlers=[
                            logging.FileHandler(logfile),
                            logging.StreamHandler()
                        ] 
        )
        
    def prepend_extra_lines_csv(self, number_of_extra_lines, csv_file):
        with open(csv_file, 'r+') as file:
                readcontent = file.read()  # store the read value of exe.txt into 
                                    # readcontent 
                file.seek(0, 0) #Takes the cursor to top line
                file.write(number_of_extra_lines*"\n") #convert int to str since write() deals 
                                   # with str
                file.write(readcontent) #after content of string are written, I return 
                                 #back content that were in the file
    
    
    
    def construct_datatime_from_input(self, start_time, start_date, end_time, end_date):
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
        
        return st, et

    def check_of_file_within_date_time_range(self, cfile, st, et):
        modifiedtstamp = datetime.fromtimestamp(os.path.getmtime(cfile))

        if st < modifiedtstamp < et:
            self.logger.info(f"This file is to be included : {cfile}")
        else:
            self.logger.info(f"discard")