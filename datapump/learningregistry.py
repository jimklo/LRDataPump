from urlparse import urlparse
import logging, urllib2

log = logging.getLogger(__name__)

class LearningRegistry():
    def __init__(self, identity, config):
        self.identity = identity
        self.config = config

    def validate_resource_locator(self, resource_locator):
        if resource_locator == None or len(resource_locator) == 0:
            log.info("Skipping: No resource_locator")
            return  False        
        try:
            (scheme, netloc, _, _, _, _) = urlparse(resource_locator)
            if scheme == '' or netloc == '':
                log.info("Skipping: Bad resource_locator")
                return False
        except:
            log.exception("Not a URL: %s", resource_locator)
            return False
        
        try:
            f = urllib2.urlopen(resource_locator)
            return True
        except:
            return False


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