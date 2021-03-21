# -*- coding: utf-8 -*-
"""
Created on Wed Mar 17 09:47:12 2021

@author: SANTIAGO
"""
# import sys
from Components.Processor import Processor
# message = "\nhola soy el script de procesamiento de python"
# print(message)
# sys.stdout.flush()

if (__name__ == "__main__"):            
    # Beware with the slash, path must be with "/" or "\\"
    source_folder = "D:/Laboral/Vitalbox/Proyectos/Clientes/Katia/Procesamiento Carga Masiva/Archivos Fuent/" 
    summary_file = "ResumenProcesamiento.txt"
    delimiter = '|'
    threshold = 1   # Amount of files needed to run the process
    table_target = "enroladoss"
    url = None      # URL of the service to consume
    
    processor = Processor(source_folder=source_folder, delimiter=delimiter, table_target=table_target, threshold=threshold, url=url, summary_file=summary_file)
    processor.run()
    
    # message = "hola soy el script de procesamiento de python"
    # print(message)
    # sys.stdout.flush()


       





 