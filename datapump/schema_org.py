
from learningregistry import LearningRegistry
import hashlib, logging, json

log = logging.getLogger(__name__)



class MicrodataPlusJSON(LearningRegistry):
    def __init__(self, run):
        LearningRegistry.__init__(self, run.identity, run.config)

    def _get_template(self, resource_locator):
        template = self.get_doc_template()
        template["payload_placement"] = "inline"
        template["payload_schema"] = [ "schema.org", "LRMI", "application/microdata+json" ]
        template["payload_schema_locator"] = "http://www.w3.org/TR/2012/WD-microdata-20121025/#converting-html-to-other-formats"
        template["resource_locator"] = resource_locator
        return template

    def format(self, record):

        item = record['items'][0]

        resource_locator = item["id"]

        if not self.validate_resource_locator(resource_locator):
            return (None, None)

        doc = self._get_template(resource_locator)

        doc["resource_data"] = record

        for key in doc.keys():
            if (doc[key] == None):
                del doc[key]
                
        # signer has a problem with encoding descendents of string type
        docrepr = json.dumps(doc)
        doc = json.loads(docrepr)

        
        return ("{0}:{1}".format(hashlib.sha1(docrepr).hexdigest(),resource_locator), doc)

