
import copy, ijson, json, logging, urllib2, sys
import commoncore

log = logging.getLogger(__name__)

def make_author_property(name):
    author = {
        "type": ["http://schema.org/Person"],
        "properties": {
            "name": [name]
        }
    }
    return author


def make_video_property(item):
    video = {
        "type": ["http://schema.org/VideoObject"],
        "properties": {}
    }

    if "youtube_id" in item:
        video["id"] = [item["youtube_id"]]
        video["playerType"] = ["youtube"]

    props = video["properties"]
    if "url" in item:
        props["url"] = [item["url"]]

    if "download_urls" in item:
        dl = item["download_urls"]
        if dl is not None:
            if "mp4" in dl:
                props["contentUrl"] = [dl["mp4"]]
                props["encodingFormat"] = ["mpeg4"]
            if "png" in dl:
                props["thumbnailUrl"] = [dl["png"]]

    if "duration" in item:
        item["duration"] = ["PT{0}S".format(item["duration"])]

    return video



def make_schema_org(std):

    real_std = commoncore.ccss.getFromCurrent(std['cc_url'])
    if real_std == None:
        real_std = commoncore.ccss.getFromCurrentHash(std['cc_url'])

    if real_std != None:
        cc_id = real_std['GUID']
        cc_url = real_std['URI']
        cc_name = real_std['Dot Notation']
    else:
        cc_id = None
        cc_url = std['cc_url']
        cc_name = std['standard']

    item_template = {
        "type": ["http://schema.org/CreativeWork"],
        "id": None,
        "properties": {
            "url": [  ],
            "name": [  ],
            "learningResourceType": [],
            "educationalAlignment": [
                {
                    "type": ["http://schema.org/AlignmentObject"],
                    "id": cc_id,
                    "properties": {
                        "alignmentType": [ "teaches" ],
                        "targetUrl": [ cc_url ],
                        "targetName": [ cc_name ],
                        "targetDescription": [ std['cc_description'] ],
                        "educationalFramework": ["Common Core State Standards"]
                    }
                }
            ]
        }
    }

    if cc_id == None:
        del item_template['properties']['educationalAlignment'][0]['id']
        item_template['properties']['educationalAlignment'][0]['description'] = ["Uses an unknown legacy targetUrl. See http://www.corestandards.org/common-core-state-standards-official-identifiers-and-xml-representation"]
        sys.stderr.write("{0} {1}\n".format(cc_name, cc_url))


    def make_copy(s, type):
        item = json.loads(json.dumps(item_template))
        ka_url = s['ka_url'] 
        try:
            title = s['title']
        except:
            title = s['display_name']

        item['id'] = ka_url
        item['properties']['url'].append(ka_url)
        item['properties']['name'].append(title)
        item['properties']['learningResourceType'].append(type)

        if "author_names" in s:
            item['properties']['author'] = []
            for a in s["author_names"]:
                item['properties']['author'].append(make_author_property(a))
        elif "author_name" in s:
            item['properties']['author'] = [make_author_property(s['author_name'])]

        if "keywords" in s:
            item['properties']['keywords'] = [s['keywords']]

        if "views" in s:
            item['properties']['interactionCount'] = ['{0} UserPlays'.format(s['views'])]

        if "creation_date" in s:
            item['properties']['dateCreated'] = [s['creation_date']]

        if "date_added" in s:
            item['properties']['datePublished'] = [s['date_added']]

        if "thumbnail_image" in s:
            item['properties']['thumbnailUrl'] = [s['thumbnail_image']]


        if "download_urls" in s:
            dl = s['download_urls']
            if dl is not None and "png" in dl:
                item['properties']['thumbnailUrl'] = [ dl['png'] ]

        if "kind" in s and s["kind"] == "Video":
            item['properties']['video'] = [ make_video_property(s) ]

        if "description" in s and s["description"] != None and s["description"].strip() != "":
            item['properties']['description'] = [ s["description"] ]



        return item

    if len(std['videos']) > 0:
        for video in std['videos']:
            i = make_copy(video, 'video')
            if i != None:
                yield { "items": [i] }
    if len(std['exercises']) > 0:
        for exercise in std['exercises']:
            i = make_copy(exercise, 'exercise')
            if i != None:
                yield { "items": [i] }
        





class Fetcher():
    def __init__(self, namespaces=None, conf=None):

        # self.endpoint = "http://www.khanacademy.org/api/v1/commoncore?lightweight=1&structured=1"
        # self.endpoint = "http://www.khanacademy.org/api/v1/commoncore?lightweight=1"
        self.endpoint = "http://www.khanacademy.org/api/v1/commoncore"
        self.endpoint = "file:/Users/jklo/projects/lr/workspace/DataPump/commoncore"
        pass

    def fetchEarliestDatestamp(self):
        return None

    def fetchRecords(self):

        standards = ijson.items(urllib2.urlopen(self.endpoint), 'item')

        for std in standards:
            if ('videos' in std and len(std['videos']) > 0) or ('exercises' in std and len(std['exercises']) > 0):
                for res in make_schema_org(std):
                    yield [ res ]
  

    def tostring(self, rec):
        return json.dumps(rec).encode('utf-8')

    def load(self, f):
        return json.load(f)



if __name__ == '__main__':
    f = Fetcher()

    print '[\n{0}\n]'.format("\n,\n".join(map(lambda x: f.tostring(x), list(f.fetchRecords()))))

    
  
