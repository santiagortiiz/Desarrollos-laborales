# -*- coding: utf-8 -*-
"""
Created on Tue Mar  9 20:25:07 2021

@author: SANTIAGO
"""

import csv

#%%
def readLinesOf(path):
    for line in open(path, 'rt', encoding="utf-8"):
        yield line # Returns the line and stand by the function

#%%
path = "D:/Laboral/Vitalbox/Proyectos/Clientes/Katia/Procesamiento Carga Masiva/Archivos/RECLAMACIONES_01032021_7244978.txt"
filteredPath = "D:/Laboral/Vitalbox/Proyectos/Clientes/Katia/Procesamiento Carga Masiva/Archivos/RECLAMACIONES_FILTRADAS_01032021_7244978.txt"
statisticsPath = "D:/Laboral/Vitalbox/Proyectos/Clientes/Katia/Procesamiento Carga Masiva/RECLAMACIONES_ESTADISTICAS.txt"

#%%
lines = readLinesOf(path) # Generator instance

header = next(lines).strip().split('|')
estatus_ht_index = header.index('estatus_ht')
print(header)

#%%
limit = 5000
evaluados = 0
enrolados = 0
inactivos = 0

with open(filteredPath, 'w', newline='') as file: # Automatic Open, Enter, Close
    writer = csv.writer(file)
    writer.writerow(header)
    for line in lines:
        line = line.strip().split('|') 
        
        if (line[estatus_ht_index] != ''):
            writer.writerow(line)
            enrolados += 1
        
        else:
            inactivos += 1
            
        evaluados += 1
        #if (enrolados == limit):
        #    break
            
print('\n')
print(evaluados, enrolados, inactivos)

with open(statisticsPath, 'a') as file:
    file.write(evaluados, enrolados, inactivos)
