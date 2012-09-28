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
Created on Oct 20, 2011

@author: jklo
'''
from StringIO import StringIO
from lxml import etree
from urllib import urlencode
from urlparse import urlparse
import logging
import sys
import time
import urllib2

log = logging.getLogger(__name__)

class OAIDC():
    def __init__(self, identity, config, namespaces):
        self.config = config
        self.identity = identity
        self.namespaces = namespaces
        
    def _unique(self, seq):
        # Not order preserving
        return list(set(seq))
    
    def _setLRTestData(self, doc):
        if doc != None and self.config.has_key("lr-test-data") and self.config["lr-test-data"] == True:
            doc["keys"].append("lr-test-data")
        return doc
        
    def get_doc_template(self):
        doc = { 
                "doc_type": "resource_data", 
                "doc_version": "0.23.0", 
                "resource_data_type" : "metadata",
                "active" : True,
                "identity": self.identity,
                "TOS": {
                        "submission_TOS":    self.config["tos"]
                },
                "resource_locator": None,
                "keys": [],
                "payload_placement": None,
                "payload_schema": [],
                "payload_schema_locator":None,
                "payload_locator": None,
                "resource_data": None
                }
        if  "attribution" in self.config and self.config["attribution"] is not None:
            doc["TOS"]["submission_attribution"] = self.config["attribution"]
        return doc
    
    def format(self, record):

        doc = self.get_doc_template()
        resource_locator = record.xpath("oai:metadata/oai_dc:dc/dc:identifier/text()", namespaces=self.namespaces)
        
        if resource_locator == None or len(resource_locator) == 0:
            return (None, None)
        
        subject = record.xpath("oai:metadata/oai_dc:dc/dc:subject/text()", namespaces=self.namespaces)
        language = record.xpath("oai:metadata/oai_dc:dc/dc:language/text()", namespaces=self.namespaces)
        payload = record.xpath("oai:metadata/oai_dc:dc", namespaces=self.namespaces)
        
        try:
            repo_id = record.xpath("oai:header/oai:identifier[1]/text()", namespaces=self.namespaces)[0]
        except:
            repo_id = None
        
        doc["resource_locator"] = resource_locator[0].strip()
        
        doc["keys"].extend(map(lambda x: str(x).strip(), subject))
        doc["keys"].extend(map(lambda x: str(x).strip(), language))
        doc = self._setLRTestData(doc)
        doc["keys"] = self._unique(doc["keys"])
        
        doc["payload_schema"].append("oai_dc")
        doc["payload_schema_locator"] = "http://www.openarchives.org/OAI/2.0/oai_dc.xsd"
        
        doc["payload_placement"] = "inline"
        doc["resource_data"] = etree.tostring(payload[0]).strip()
        
        for key in doc.keys():
            if (doc[key] == None):
                del doc[key]
                
        # signer has a problem with encoding descendents of string type
        doc = eval(repr(doc))
        
        return (repo_id, doc)


class NSDL(OAIDC):
    def __init__(self, identity, config, namespaces, col_map=None):
        OAIDC.__init__(self, identity, config, namespaces)
        self.col_map = {}
        try:
            self.col_map.update(col_map)
        except:
            pass
        
    def format(self, record):
        doc = self.get_doc_template()
        resource_locator = record.xpath("oai:metadata/nsdl_dc:nsdl_dc/dc:identifier/text()", namespaces=self.namespaces)
        
        if resource_locator == None or len(resource_locator) == 0:
            log.info("Skipping: No resource_locator")
            return (None, None)
        
        try:
            (scheme, netloc, _, _, _, _) = urlparse(resource_locator[0])
            if scheme == '' or netloc == '':
                log.info("Skipping: Bad resource_locator")
                return (None, None)
        except:
            log.exception("Not a URL: %s", resource_locator[0])
            return (None, None)
        
        try:
            repo_id = record.xpath("oai:header/oai:identifier[1]/text()", namespaces=self.namespaces)[0]
        except:
            repo_id = None
        
        
        
        subject = record.xpath("oai:metadata/nsdl_dc:nsdl_dc/dc:subject/text()", namespaces=self.namespaces)
        language = record.xpath("oai:metadata/nsdl_dc:nsdl_dc/dc:language/text()", namespaces=self.namespaces)
        edLevel = record.xpath("oai:metadata/nsdl_dc:nsdl_dc/dct:educationLevel/text()", namespaces=self.namespaces)
        payload = record.xpath("oai:metadata/nsdl_dc:nsdl_dc", namespaces=self.namespaces)
        collection = record.xpath("oai:header/oai:setSpec[1]/text()", namespaces=self.namespaces)
        schemaLocation = record.xpath("oai:metadata/nsdl_dc:nsdl_dc/@xsi:schemaLocation", namespaces=self.namespaces)
        
        doc["resource_locator"] = resource_locator[0].strip()
        
        doc["keys"].extend(map(lambda x: str(x).strip(), subject))
        doc["keys"].extend(map(lambda x: str(x).strip(), language))
        doc["keys"].extend(map(lambda x: str(x).strip(), edLevel))
        
        doc["keys"].extend(map(lambda x: str(x).strip(), collection))
        if len(collection) > 0 and collection[0].strip() in self.col_map:
            doc["keys"].append(self.col_map[collection[0]])
            
        doc = self._setLRTestData(doc)
        doc["keys"] = self._unique(doc["keys"])
        
        doc["payload_schema"].append("nsdl_dc")
        doc["payload_schema_locator"] = schemaLocation[0].strip()
        
        doc["payload_placement"] = "inline"
        doc["resource_data"] = etree.tostring(payload[0]).strip()
        
        for key in doc.keys():
            if (doc[key] == None):
                del doc[key]
                
        # signer has a problem with encoding descendents of string type
        doc = eval(repr(doc))
        
        return (repo_id, doc)

class Fetcher():
    def __init__(self, namespaces=None, conf=None):
        self.WAIT_DEFAULT = 120 # two minutes
        self.WAIT_MAX = 5
        self.namespaces = {
              "oai" : "http://www.openarchives.org/OAI/2.0/",
              "oai_dc" : "http://www.openarchives.org/OAI/2.0/oai_dc/",
              "dc":"http://purl.org/dc/elements/1.1/",
              "dct":"http://purl.org/dc/terms/",
              "nsdl_dc":"http://ns.nsdl.org/nsdl_dc_v1.02/",
              "ieee":"http://www.ieee.org/xsd/LOMv1p0",
              "xsi":"http://www.w3.org/2001/XMLSchema-instance"
              }
        try:
            self.namespaces.update(namespaces)
        except:
            log.exception("Unable to merge specified namespaces")
            
        self.conf = {
            "harvest_from": None, 
            "harvest_until": None
        }
        try:
            self.conf.update(conf)
        except:
            log.exception("Unable to merge conf")
    
    def fetchEarliestDatestamp(self):
        server = self.conf["server"]
        path = self.conf["path"]
        
        params = {
                  "verb": "Identify"
        }
        
        body = self.makeRequest("%s%s" % (server, path), **params)
        f = StringIO(body)
        tree = etree.parse(f)
        
        try:
            earliestDatestamp = tree.xpath("oai:Identify/oai:earliestDatestamp/text()", namespaces=self.namespaces)
            early =  earliestDatestamp[0]
        except:
            early = "1900-01-01T00:00:00Z"
        
        return early
    
    def fetchCollections(self):
        server = self.conf["server"]
        path = self.conf["path"]
        
        params = {
                  "verb": "ListSets"
        }
        
        body = self.makeRequest("%s%s" % (server, path), **params)
        f = StringIO(body)
        tree = etree.parse(f)
        list_sets = tree.xpath("oai:ListSets/oai:set", namespaces=self.namespaces)
        
        col_names = {}
        for col in list_sets:
            spec = col.xpath("oai:setSpec[1]/text()", namespaces=self.namespaces)
            name = col.xpath("oai:setName[1]/text()", namespaces=self.namespaces)
            col_names[spec[0].strip()] = name[0].strip()
        
        return col_names

    
    def fetchRecords(self):
        '''
        Generator to fetch all records using a resumptionToken if supplied.
        ''' 
        server = self.conf["server"]
        path = self.conf["path"]
        verb = self.conf["verb"]
        metadataPrefix = self.conf["metadataPrefix"]
        set = self.conf["set"]
        
        params = { "verb": verb, "metadataPrefix": metadataPrefix }
        tok_params = { "verb": verb }
        if set != None:
            params["set"] = set
            
        if self.conf["harvest_from"] != None:
            params["from"] = self.conf["harvest_from"]
        if self.conf["harvest_until"] != None:
            params["until"] = self.conf["harvest_until"]
        
        body = self.makeRequest("%s%s" % (server, path), **params)
        f = StringIO(body)
        tree = etree.parse(f)
        tokenList = tree.xpath("oai:ListRecords/oai:resumptionToken/text()", namespaces=self.namespaces)
        log.debug("FIRST RESUMPTION: "+str(tokenList))
        yield tree.xpath("oai:ListRecords/oai:record", namespaces=self.namespaces)
        resumptionCount = 1   
        while (len(tokenList) == 1):
            try:
                resumptionCount += 1
                tok_params["resumptionToken"] = tokenList[0]
                body = self.makeRequest("%s%s" % (server, path), **tok_params)
                f = StringIO(body)
                tree = etree.parse(f)
                yield tree.xpath("oai:ListRecords/oai:record", namespaces=self.namespaces)
                tokenList = tree.xpath("oai:ListRecords/oai:resumptionToken/text()", namespaces=self.namespaces)
                log.debug("HAS RESUMPTION #{0}: {1}".format(resumptionCount, str(tokenList)))
            except:
                tokenList = []
                log.exception("Problem trying to get next segment.")
    
    def makeRequest(self, base_url, credentials=None, **kw):
        """Actually retrieve XML from the server.
        """
        # XXX include From header?
        headers = {
                   'User-Agent': 'Learning Registry Data Pump',
                   'Content-Type': 'text/xml; charset=utf-8'
        }
        if credentials is not None:
            headers['Authorization'] = 'Basic ' + credentials.strip()
        request = urllib2.Request(
            "{url}?{query}".format(url=base_url, query=urlencode(kw)), headers=headers)
        log.debug("URL Requested: %s", request.get_full_url())
        return self.retrieveFromUrlWaiting(request)
    
    def retrieveFromUrlWaiting(self, request,
                               wait_max=None, wait_default=None):
        """Get text from URL, handling 503 Retry-After.
        """
        if not wait_max:
            wait_max = self.WAIT_MAX
        
        if not wait_default:
            wait_default = self.WAIT_DEFAULT
            
        for i in range(wait_max):
            try:
                f = urllib2.urlopen(request)
                text = f.read()
                f.close()
                # we successfully opened without having to wait
                break
            except urllib2.HTTPError, e:
                if e.code == 503:
                    try:
                        retryAfter = int(e.hdrs.get('Retry-After'))
                    except TypeError:
                        retryAfter = None
                    if retryAfter is None:
                        time.sleep(wait_default)
                    else:
                        time.sleep(retryAfter)
                else:
                    # reraise any other HTTP error
                    raise
        else:
            raise Exception, "Waited too often (more than %s times)" % wait_max
        return text            