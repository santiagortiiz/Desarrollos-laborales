# -*- coding: utf-8 -*-
"""
Created on Tue Mar  9 21:04:50 2021

@author: SANTIAGO
"""

import csv
import os

def readLinesOf(path):
    for line in open(path, 'rt', encoding="utf-8"):
        yield line # Returns the line and suspend the function

#%%
sourceFolder = "ArchivosDescargados"
resultFolder = "ArchivosFiltrados" # File where the information resulting from the processing will be saved
statisticFile = "RECLAMACIONES_ESTADISTICAS"

sourcePath = f"D:/Laboral/Vitalbox/Proyectos/Clientes/Katia/Procesamiento Carga Masiva/{sourceFolder}/"
#resultPath = f"D:/Laboral/Vitalbox/Proyectos/Clientes/Katia/Procesamiento Carga Masiva/{resultFolder}/"
summaryPath = f"D:/Laboral/Vitalbox/Proyectos/Clientes/Katia/Procesamiento Carga Masiva/{statisticFile}.txt"

files = os.listdir(sourcePath)
reclamaciones = [sourcePath + i for i in files if 'FILTRADAS' not in i]
reclamacionesFiltradas = [sourcePath + i[:14] + "FILTRADAS" + i[13:] for i in files]

#%%
for i in range(len(reclamaciones)):
    
    lines = readLinesOf(reclamaciones[i])  # Generator instance

    evaluados = 0                       # Init Statistics variables
    enrolados = 0
    inactivos = 0
    
    header = next(lines).strip().split('|')         # Transform the first row to a header
    estatus_ht_index = header.index('estatus_ht')   # Determine the index of estatus_ht

    with open(reclamacionesFiltradas[i], 'w', newline='') as file: # Automatic Open, Enter, Close
        writer = csv.writer(file, delimiter='|')
        writer.writerow(header)
        for line in lines:
            line = line.strip().split('|') 
            #print(f"\n{len(split_line)}\n{split_line}\n\nOriginal line:\n{line}")
            
            if (line[estatus_ht_index] != ''):
                writer.writerow(line)
                enrolados += 1
            
            else:
                inactivos += 1
                
            evaluados += 1
            #if (enrolados == limit):
            #    break
    
    with open(summaryPath, 'a') as file:
        string = "\n{}, {}, {}, {}".format(reclamaciones[i][92:122], evaluados, enrolados, inactivos)
        file.write(string)
   
    print(f"File: {reclamaciones[i][92:122]} - OK")
