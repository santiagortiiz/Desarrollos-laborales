# -*- coding: utf-8 -*-
"""
Created on Wed Mar 17 08:40:32 2021

@author: SANTIAGO
"""

import requests
#import json

#%%
class Requests_Engine(object):                                                
    def __init__(self, url):
        self.url = url
        self.endpoint = None
        self.cookies = None

    
    def GET(self, **kwargs):
        response = requests.get(self.url, kwargs)
        
        if response.status_code == 200:
            return response.json()
    
    def POST(self, **kwargs):
        payload = kwargs.get("payload", None)
        response = requests.post(self.url, json=payload)
        
        if response.status_code == 200:
            return response.json()
    
        
        
        
        
        
        
        
        
        
        
        
        
        
        