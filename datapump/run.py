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
from __future__ import division
from LRSignature.sign import Sign
from datapump.couchdb import CouchDB
from datapump.oaipmh import NSDL, OAIDC
from datetime import datetime
from filelock.filelock import FileLock, FileLockException
from urllib2 import HTTPError
import argparse
import codecs
import datapump.couchdb
import json
import logging
import oaipmh
import os
import sys
import urllib2
import base64

'''
Created on Oct 20, 2011

@author: jklo
'''

log = logging.getLogger(__name__)

def LogStartStop():
    
    start = datetime.utcnow()
    
    def log_start():
        log.info("Started @ {0}".format(start.strftime("%Y-%m-%dT%H:%M:%SZ")))
    
    def log_info(obj_opts):
        config = obj_opts.settings["config"]

        log.info("Harvesting data from: {0}{1} from: {2} until: {3}".format(config["server"], config["path"], config["harvest_from"], config["harvest_until"]))
        log.info("Publishing to: {0}".format(obj_opts.LEARNING_REGISTRY_URL))
    
    def log_error():
        log.exception("An uncaught error occurred")
    
    def log_finish():
        finish = datetime.utcnow()
        dur = finish - start
        dur_secs = (dur.microseconds + (dur.seconds + dur.days * 24 * 3600) * 10**6) / 10**6
        log.info("Finished @ {0}, Duration: {1} seconds".format(finish.strftime("%Y-%m-%dT%H:%M:%SZ"), dur_secs))
    
    
    def decorator(fn):
        def wrapped_fn(self, *args, **kw):
            try:
                log_start()
                log_info(self.opts)
                fn(self, *args, **kw)
            except:
                log_error()
            finally:
                log_finish()
        return wrapped_fn
    return decorator


class Opts:
    def __init__(self):
        self.LEARNING_REGISTRY_URL = None
        
        parser = argparse.ArgumentParser(description="Harvest OAI data from one source and convert to Learning Registry resource data envelope. Write to file or publish to LR Node.")
        parser.add_argument('-u', '--url', dest="registryUrl", help='URL of the registry to push the data. "-" prints to stdout', default=self.LEARNING_REGISTRY_URL)
        parser.add_argument('-c', '--config', dest="config", help='Configuration file', default=None)
        parser.add_argument('--chunksize', help='Number of envelopes per output file', type=int, default=100)
        options = parser.parse_args()
        self.LEARNING_REGISTRY_URL = options.registryUrl
        self.CONFIG_FILE = options.config
        self.OPTIONS = options
        self.CHUNKSIZE = options.chunksize
        
        self._config = {
            "server": "",
            "path": "",
            "verb": None,
            "metadataPrefix":None,
            "set":"",
            "tos": "http://nsdl.org/help/?pager=termsofuse",
            "attribution": None,
            "sign": False,
            "lr-test-data": True
#            "keyId": None,
#            "passphrase": "",
#            "keyLocations": None
        }
        
        self._identity = {            
            "submitter_type": "agent",
            "submitter": "NSDL 2 LR Data Pump"
        }
        
        self._namespaces = {
            "oai" : "http://www.openarchives.org/OAI/2.0/",
            "oai_dc" : "http://www.openarchives.org/OAI/2.0/oai_dc/",
            "dc":"http://purl.org/dc/elements/1.1/",
            "dct":"http://purl.org/dc/terms/",
            "nsdl_dc":"http://ns.nsdl.org/nsdl_dc_v1.02/",
            "ieee":"http://www.ieee.org/xsd/LOMv1p0",
            "xsi":"http://www.w3.org/2001/XMLSchema-instance"
        }
        
        self.signtool = None
        self.settings = {"config":self._config, "namespaces":self._namespaces, "identity":self._identity}
        
        self.readConfig()
        self.parseSettings()
    
    def readConfig(self):
        
        if self.CONFIG_FILE != None and os.path.exists(self.CONFIG_FILE):
            
            extConf = json.load(file(self.CONFIG_FILE))
                
            self.settings.update(extConf)
            
        config = self.settings["config"]
        try:
            if config["sign"] == True and "keyId" in config and "passphrase" in config and "keyLocations" in config:
                
                gpg = {
                       "privateKeyID": config["keyId"],
                       "passphrase": config["passphrase"],
                       "publicKeyLocations": config["keyLocations"]
                }
                if "gnupgHome" in config:
                    gpg["gnupgHome"] = config["gnupgHome"]
                
                if "gpgbin" in config:
                    gpg["gpgbin"] = config["gpgbin"]
    
                self.signtool = Sign.Sign_0_21(**gpg)
        except:
            log.exception("Error with signing configuration.")
            
        try:
            if self.LEARNING_REGISTRY_URL==None and config["publish_url"] != None:
                self.LEARNING_REGISTRY_URL = config["publish_url"]
        except:
            if self.LEARNING_REGISTRY_URL==None:
                self.LEARNING_REGISTRY_URL="-"

                
    
    def parseSettings(self):
        
        fetcher = oaipmh.Fetcher(namespaces=self.settings["namespaces"], conf=self.settings["config"])
        
        try:
            self.prev_success = self.settings["config"]["harvest_completed"]
        except:
            self.prev_success = False
            self.settings["config"]["harvest_completed"] = self.prev_success
        
        try:
            self.from_date = self.settings["config"]["harvest_from"]
        except:
            self.from_date = fetcher.fetchEarliestDatestamp()
            self.settings["config"]["harvest_from"] = self.from_date
            self.prev_success = False
        
        try:
            self.until_date = self.settings["config"]["harvest_until"]
        except:
            now = datetime.utcnow()
            self.until_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            self.settings["config"]["harvest_until"] = self.until_date
            self.prev_success = False
            
        if self.prev_success:
            self.from_date = self.until_date
            self.settings["config"]["harvest_from"] = self.from_date
            now = datetime.utcnow()
            self.until_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")    
            self.until_date = self.settings["config"]["harvest_until"] = self.until_date
            
    

class Run():
    def __init__(self, opts=Opts()):
        self.opts = opts
        
        self.namespaces = opts.settings["namespaces"]
        self.config = opts.settings["config"]
        self.identity = opts.settings["identity"]
        self.signtool = opts.signtool
        
        
        self.fetcher = oaipmh.Fetcher(namespaces=opts.settings["namespaces"], conf=opts.settings["config"])
        self.docList = {}
       
        if self.opts.settings["config"]["metadataPrefix"] == "nsdl_dc":
            col_names = self.fetcher.fetchCollections()
            self.transformer = NSDL(identity=self.identity, config=self.config, namespaces=self.namespaces, col_map=col_names)
        elif self.opts.settings["config"]["metadataPrefix"] == "oai_dc":
            self.transformer = OAIDC(identity=self.identity, config=self.config, namespaces=self.namespaces)
        else:
            self.transformer = None
        
        try:
            self.prev_success = self.opts.prev_success
        except:
            self.prev_success = False
        
        try:
            self.from_date = self.opts.from_date
        except:
            self.from_date = self.fetcher.fetchEarliestDatestamp()
            self.prev_success = False
        
        try:
            self.until_date = self.opts.until_date
        except:
            now = datetime.utcnow()
            self.until_date = now.strftime("%Y-%m-%dT%H:%M:%SZ")
            self.prev_success = False
            

        self.couch = CouchDB(self.opts, self.from_date, self.until_date)
        self.completed_set = False
    
    def getPublishEndpoint(self):
        if self.opts.LEARNING_REGISTRY_URL == "-":        
            self.publishEndpoint = None
            self.chunk = 0
        else:
            hdrs = {"Content-Type":"application/json; charset=utf-8"}
            
            try:
                if self.config["publish_user"] is not None and self.config["publish_passwd"] is not None:
                    creds = "{u}:{p}".format(u=self.config["publish_user"].strip(), p=self.config["publish_passwd"].strip())
                    hdrs['Authorization'] = 'Basic ' + base64.encodestring(creds)[:-1]
            except:
                pass
            
            self.publishEndpoint = urllib2.Request("{server}/publish".format(server=self.opts.LEARNING_REGISTRY_URL), headers=hdrs)
        
        return self.publishEndpoint 
 
    def sign(self, doc):
        if doc != None and self.signtool != None:
            signed = self.signtool.sign(doc)
            try:
                if len(signed["digital_signature"]["signature"]) == 0:
                    log.error("Problem signing document")
            except:
                log.exception("There's a problem with the digital_signature")
            
            return signed
        else:
            return doc
        
    @LogStartStop()
    def connect(self):
        try:
            for recset in self.fetcher.fetchRecords():
                for rec in recset:
                    if self.transformer is not None:
                        (repo_id, doc) = self.transformer.format(rec)
                        seen = self.couch.have_i_seen(repo_id)
                        if not seen or seen["published"] == False:
                            doc = self.sign(doc)
                            if (doc != None and repo_id != None):
                                self.docList[repo_id] = doc
    
                    self.publishToNode()
                self.publishToNode()
            self.publishToNode(force=True)
            self.completed_set = True
        except:
            log.exception("Stopping")
        finally:
            self.storeHistory()
            
    def storeHistory(self):
        
        state = { 
                 "harvest_completed": self.completed_set,
                 "harvest_from": self.from_date,
                 "harvest_until": self.until_date
        }
        
        if self.completed_set:
            self.couch.forget_everything()
        
        if self.opts.CONFIG_FILE != None and os.path.exists(self.opts.CONFIG_FILE):
            
            extConf = json.load(codecs.open(self.opts.CONFIG_FILE, "r", encoding="utf-8"))
            extConf["config"].update(state)
            
            
            with codecs.open(self.opts.CONFIG_FILE, "w", encoding="utf-8") as out:
                out.write(json.dumps(extConf, indent=4, sort_keys=True))
            
            
    def publishToNode(self, force=False):
        '''
        Save to Learning Registry
        '''
        numDocs = len(self.docList.keys())
        if self.opts.CHUNKSIZE <= numDocs or (numDocs > 0 and force):
            try:
                repo_ids = []
                docList = []
                map(lambda x: (repo_ids.append(x[0]), docList.append(x[1])), self.docList.items())
                body = { "documents":docList }
                endpoint = self.getPublishEndpoint()
                if endpoint is not None:
                    response = urllib2.urlopen(endpoint, data=json.dumps(body))
                    
                    publishStatus = json.load(response)
                    if not publishStatus["OK"]:
                        log.error(publishStatus["error"])
                    
                    nonpubcount = 0 
                    for idx, result in enumerate(publishStatus["document_results"]):
                        repo_id = repo_ids[idx]
                        if not result["OK"]:
                            nonpubcount += 1
                            if "doc_ID" not in result:
                                result["doc_ID"] = "Unknown ID"
                            if "error" not in result:
                                result["error"] = "Unknown publishing error."
                            published = False
                            log.error("REPOID:{repoid} DOCID:{docid} ERROR: {msg}".format(repoid=repo_id, docid=result["doc_ID"], msg=result["error"]))
                        else:
                            published = True
                        self.couch.saw(repo_id, published)
                     
                    pubcount = numDocs - nonpubcount
                    try:
                        size = sys.getsizeof(self.docList, -1) / 1024
                    except:
                        size = 0
                    log.info("Published {pub} documents ({kbytes}KB), {nonpub} documents were not published.".format(pub=pubcount, nonpub=nonpubcount, kbytes=size))
#                    assert True == False
                else:
                    self.chunk += 1
                    print "/********* CHUNK {chunkNumber} *********/".format(chunkNumber=self.chunk)   
                    print json.dumps(body, indent=4)   
                            
                self.docList.clear()
                

            except HTTPError as e:
                log.error("HTTP Error encoutered:{0}  message:{1}".format(e.errno, e.strerror))
                raise
            except Exception:
                log.exception("Unexpected error while trying to publish to node.")
                raise
        else:
            log.debug("Nothing is being updated.")

    

if __name__ == '__main__':
    opts = Opts()
    lockfile = "%s.lck" % opts.CONFIG_FILE
    logging.basicConfig(format="%(asctime)s : %(levelname)s : %(message)s", datefmt='%Y-%m-%dT%H:%M:%S%Z', level=logging.INFO)
    try:
        with FileLock(lockfile) as fl:
            run = Run(opts)
            run.connect()
    except FileLockException as fle:
        log.info("Already Running")
    except Exception:
        log.exception()