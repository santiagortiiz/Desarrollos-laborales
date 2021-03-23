# -*- coding: utf-8 -*-
"""
Created on Wed Mar 17 09:47:12 2021

@author: SANTIAGO
"""

from Components.Processor import Processor

if (__name__ == "__main__"):            
    # Beware with the slash, path must be with "/" or "\\"
    source_folder = "D:/Laboral/Vitalbox/Proyectos/Clientes/Katia/Procesamiento Carga Masiva/Archivos Fuent/" 
    summary_file = "ResumenProcesamiento.txt"
    delimiter = '|'                 # Delimiter of the files to be processed
    threshold = 1                   # Amount of files needed to run the process
    table_target = "enroladoss"     # Name of the table in which the data will be storage
    keyword = "RECLAMACION"         # Key word to search the files in the source folder
    url = None                      # URL of the service to consume
    
    processor = Processor(source_folder=source_folder, summary_file=summary_file, 
                          keyword=keyword, delimiter=delimiter, 
                          table_target=table_target, threshold=threshold, url=url)
    processor.run()
    
    del source_folder, summary_file, delimiter, keyword, threshold, table_target, url, processor
    
    # message = "\nhola soy el script de procesamiento de python"
    # print(message)
    # sys.stdout.flush()


       





 