# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 18:10:15 2021

@author: SANTIAGO
"""

#%% Main Modules
from Components.MySQL_Engine import MySQL_Engine
from Components.Requests_Engine import Requests_Engine

import os
import csv
from datetime import datetime

#%%
class Processor(object):                                                   
    def __init__(self, **kwargs):  
        self.current_folder = kwargs.get("current_folder", os.getcwd())
        self.source_folder = kwargs.get("source_folder", "Archivos Fuente")
        self.threshold = int(kwargs.get("threshold", 0))
        self.key_delimiter = kwargs.get("delimiter", '|')
        self.keywordFileFilter = kwargs.get("keyword", "RECLAMACION")
        self.table_target = kwargs.get("table_target", "enroladoss")
        self.summary_file_name = kwargs.get("summary_file", "ResumenProcesamiento.txt")
        self.url = kwargs.get("url", None)
        
        self.db = MySQL_Engine()
        self.requests = Requests_Engine(self.url)

    def setDelimiter(self, delimiter):
        self.key_delimiter = delimiter

    def setURL(self, url):
        self.requests.url = url

    def setEnvironment(self, **kwargs):                                        
        # Creating the folder where the target files will be download
        try:                                                                    
            path = os.path.join(self.current_folder, self.source_folder) 
            os.mkdir(path)
        except OSError as error:  
            print(error) 

    def readDownloadFolder(self):
        files = os.listdir(self.source_folder)
        files = [i for i in files if self.keywordFileFilter.lower() in i.lower()]
        number_of_files = len(files)
        
        return files, number_of_files
        
    def readLinesOf(self, path):
        for line in open(path, 'rt'):
            # Returns the line and suspend the function
            yield line                                                          
    
    def run(self, **kwargs):
        files, number_of_files = self.readDownloadFolder()
        
        # Validate if there are a certain number of files
        if number_of_files >= self.threshold:
            # Instance the main objects
            self.db.setConnection()
            self.db.setTableTarget(self.table_target)
            
            # Begin the processing of each file
            for i in range(number_of_files):
                # set up the main variables
                success = 0
                failures = 0
                now = datetime.now()
                file_creation_date = now.strftime("%Y/%m/%d %H:%M:%S")
                file_processing_start_time = datetime.now()
                
                # Set the path of each downloaded file 
                file_path = os.path.join(self.source_folder, files[i]) 
                
                # Create the Yield generator Instance
                lines = self.readLinesOf(file_path)                                 
                
                # Pull the header of the file
                header = next(lines).strip().split(self.key_delimiter)                                    
                additional_attributes = ["ArchivoFuente", "fechaCreacion", "FechaActualizacion"]
                self.db.setHeader(header, additional_attributes)
                
                # File Processing
                row_number = 0
                for line in lines:
                    row_number += 1
                    
                    register_creation_time = now.strftime("%Y/%m/%d %H:%M:%S")
                    additional_values = [files[i], register_creation_time, None]
                    
                    line = line.strip().split(self.key_delimiter)                                          
                    line = additional_values + line
                    
                    # Database Storage
                    status = self.db.insertRecord(line)
                    
                    if status == True:
                        success += 1
                    else:
                        print(f"Can't insert line {row_number} from file {files[i]}")
                        failures += 1
                
                # Database Commit
                try:
                    self.db.commitRecords()
                    now = datetime.now()
                    file_storage_date = now.strftime("%Y/%m/%d %H:%M:%S")
                except:
                    print("Commit Error, please review DB connection")
                
                # POST the processing summary
                try:
                    payload = {
                        "nombreArchivo": files[i],
                        "fechaCreacion": file_creation_date,
                        "fechaFinProcesamiento": file_storage_date,
                        "numeroRegistrosAlmacenados": success,
                        "numeroRegistrosFallados": failures,
                        "token": "token"
                    }
                    self.requests.POST(payload=payload)
                except:
                    print("Posting Error, please review server connection or http request")
                
                now = datetime.now()
                file_processing_end_time = datetime.now()
                file_processing_time = file_processing_end_time - file_processing_start_time
                
                # Writing processing summary in a backup file
                summary_file_path = os.path.join(self.source_folder, self.summary_file_name) 
                header = "nombreArchivo | fechaCreacion | fechaAlmacenamiento | tiempoProcesamiento | registrosExitosos | registrosFallidos"
                self.addHeaderTo(summary_file_path, header)
                string = f"\n{files[i]} | {file_creation_date} | {file_storage_date} | {file_processing_time} | {success} | {failures}"
                self.appendToFile(summary_file_path, string)
                
                # Delete Processed File
                self.deleteFile(file_path)
                
            # Close the DB when the files from the source folder had been processed
            self.db.close() 
            
        # Using Recursivity            
        # else:
        #     self.run()

#%% Filter Functions
    def filesFilter(self, file_name, header, conditional_attribute, lines):
        evaluados = 0                                                           # Init Statistics variables
        enrolados = 0
        inactivos = 0
        
        attribute_index = header.index(conditional_attribute)
        processed_file_path = os.path.join(self.result_folder, f"Processed_{file_name}") 
        
        with open(processed_file_path, 'w', newline='') as file:                # Automatic Open, Enter, Close
            writer = csv.writer(file)
            writer.writerow(header)
            for line in lines:
                line = line.strip().split('|') 
                
                if (line[attribute_index] != ''):
                    writer.writerow(line)
                    enrolados += 1
                
                else:
                    inactivos += 1
                    
                evaluados += 1
                #if (enrolados == limit):
                #    break
        
        summary = f"\n{file_name}, {evaluados}, {enrolados}, {inactivos}"
        self.appendToFile(summary)
      
    def addHeaderTo(self, path, header):
        # If the file exist, dont create a new one
        if os.path.exists(path):
            return False
        # Otherwise, Create a file with the header in the first line
        else:
            with open(path, 'w') as file:
                file.write(header)
    
    def appendToFile(self, path, string):
         with open(path, 'a') as file:
                file.write(string)
        
    def deleteFile(self, path):
        if os.path.exists(path):
            os.remove(path)
        else:
            print(f"The file {path} can not be removed")
        

