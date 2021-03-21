# -*- coding: utf-8 -*-
"""
Created on Tue Mar 16 20:05:33 2021

@author: SANTIAGO
"""

import mysql.connector
from mysql.connector import errorcode

#%%
class MySQL_Engine(object):                                                             
    def __init__(self, **kwargs):
        self.db = None
        self.cursor = None
        self.table_name = None
        self.header = None
        self.number_of_attributes = None
        self.values_format = None
        
        self.config = {       # (Key, Default Value)
            "host": kwargs.get("host", "localhost"),
            "user": kwargs.get("user", "root"),
            "password": kwargs.get("password", "r00t"),
            "port": kwargs.get("port", "3306"),
            "database": kwargs.get("database", "my_vitalbox_2"),
            "raise_on_warnings": kwargs.get("raise_on_warnings", False)
        }
        
    def setConnection(self):
        try:
            self.db = mysql.connector.connect(**self.config)                                # Connection arguments in a dictionary
            self.cursor = self.db.cursor()
        
        except mysql.connector.Error as err:                                                # Error Handling
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print(err)
    
    def setTableTarget(self, table_name):
        self.table_name = table_name
    
    def setHeader(self, header, additional_attributes):
        self.number_of_attributes = len(header) + len(additional_attributes)
        header = additional_attributes + header
        header = ', '.join(header)
        self.header = f"({header})" 
        
        values_format = ("%s, " * self.number_of_attributes)[:-2]
        self.values_format = f"({values_format})"
    
    def insertRecord(self, data_record):                                                    # Header and data_record must be tuples      
        add_record = (
            f'''
            INSERT INTO {self.table_name}                                                      
            {self.header}
            VALUES {self.values_format}
            '''
        )
        
        try:
            self.cursor.execute(add_record, data_record)
            return True
        
        except mysql.connector.Error as err:
            print("Error ingresando registro")
            print(err)
            return False
      
    def commitRecords(self):
        self.db.commit()
        
    def close(self):
        self.cursor.close()
        self.db.close()






