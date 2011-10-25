#Copyright 2011 SRI International
#
#Licensed under the Apache License, Version 2.0 (the "License");
#you may not use this file except in compliance with the License.
#You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
#Unless required by applicable law or agreed to in writing, software
#distributed under the License is distributed on an "AS IS" BASIS,
#WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#See the License for the specific language governing permissions and
#limitations under the License.
'''
Created on Oct 21, 2011

@author: jklo
'''
from urllib2 import HTTPError
import json
import urllib
import urllib2

class CouchDB(object):
    '''
    Storage Class for logging
    '''


    def __init__(self, opts=None, from_date=None, until_date=None):
        '''
        initializes CouchDB
        '''
        try:
            server = opts.settings["couch"]["server"]
        except:
            server="http://localhost:5984"
        try:
            db = opts.settings["couch"]["db"]
        except:
            db = "dpump"
            
            
        self.couch = "{server}/{db}".format(server=server, db=db)
        self.from_date = from_date
        self.until_date = until_date
        
        try:
            self._do_couch("PUT")
        except:
            pass
        
        

    def _do_couch(self, verb="GET", path="", data=None):
        
        opts = {
                "url":'{url}{path}'.format(url=self.couch, path=path)
                }
        if data:
            opts["data"] = json.dumps(data)
        
        opener = urllib2.build_opener(urllib2.HTTPHandler)
        request = urllib2.Request(**opts)
        request.add_header('Content-Type', 'application/json; charset=utf-8')
        request.get_method = lambda: verb
        
        opened = opener.open(request)
    
        return json.load(opened)
    
    
    
    def saw(self, repoid, published=True):
        
        doc ={
         "_id": repoid,
         "from": self.from_date,
         "until": self.until_date,
         "published": published
        }
        
        try:
            result = self.have_i_seen(repoid)
            rev = result["_rev"]
            
            doc["_rev"] = rev
            result = self._do_couch("PUT", data=doc)  
        except:
            result = self._do_couch("POST", data=doc)
        
        return result
    
    def have_i_seen(self, repoid):
        try:
            result = self._do_couch("GET", path="/{id}".format(id=urllib.quote(repoid, safe="")))
            if result["_id"]:
                pass
        except:
            result = None
        
        return result
    
    def forget_everything(self):
        
        result = self._do_couch("DELETE")
        
        return result
    

if __name__ == "__main__":
    couch = CouchDB()
    saw = couch.saw("abc123")
    seen = couch.have_i_seen("abc123")
    forgot = couch.forget_everything()
    
    pass
    