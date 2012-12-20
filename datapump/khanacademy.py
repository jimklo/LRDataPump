
import copy, ijson, json, logging, urllib, urllib2, urlparse, sys, tempfile
import commoncore

log = logging.getLogger(__name__)

_ka_api_commoncore = "http://www.khanacademy.org/api/v1/commoncore?lightweight=1"
_ka_api_videos = "http://www.khanacademy.org/api/v1/videos/{0}"
_ka_api_exercises = "http://www.khanacademy.org/api/v1/exercises/{0}"

_log_dates = None

def _write_log_dates(msg):
    if _log_dates != None:
        _log_dates.write(msg)
        _log_dates.flush()

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
        video["id"] = "urn:www.youtube.com:videoid:%s" % item["youtube_id"]
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

def get_details(url_str):
    details = None
    try:
        asset_type, readable_id = urlparse.urlsplit(url_str).path.split('/')[-2:]

        if asset_type.startswith("video"):
            detail_url = _ka_api_videos.format(urllib.quote(readable_id))
        elif asset_type.startswith("exercise"):
            detail_url = _ka_api_exercises.format(urllib.quote(readable_id))
        else:
            detail_url = None

        if detail_url != None:
            details = json.load(urllib2.urlopen(detail_url))
    except Exception as e:
        log.exception(e)
        log.error("detail_url: %s"%detail_url)
    return details





def make_schema_org(std, filter_from=None, filter_until=None):

    real_std = commoncore.ccss.getFromCurrent(std['cc_url'])
    if real_std == None:
        real_std = commoncore.ccss.getFromCurrentHash(std['cc_url'])

    if real_std != None:
        cc_id = "urn:corestandards.org:guid:%s"%real_std['GUID']
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


    def check_date(date):
        okay = True

        if filter_from is not None:
            okay = okay & (date >= filter_from)

        if filter_until is not None:
            okay = okay & (date < filter_until)

        log.debug("from: {0} until: {1} date: {2} okay: {3}".format(filter_from, filter_until, date, okay))
        return okay

    def make_copy(s, type):
        item = json.loads(json.dumps(item_template))
        ka_url = s['ka_url'] 
        try:
            title = s['title']
        except:
            title = s['display_name']

        det = get_details(ka_url)
        if det != None:
            s.update(det)
        else:
            return None

        item['id'] = ka_url
        item['properties']['url'].append(ka_url)
        item['properties']['name'].append(title)
        item['properties']['learningResourceType'].append(type)

        if "node_slug" in s:
            item['id'] = 'urn:www.khanacademy.org:node_slug:%s' % s['node_slug']

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

        okay = True
        if "creation_date" in s:
            okay = okay & check_date(s['creation_date'])
            _write_log_dates("c: {0}\t{1}\n".format(s['creation_date'],okay))
            item['properties']['dateCreated'] = [s['creation_date']]

        if "date_added" in s:
            okay = okay & check_date(s['date_added'])
            _write_log_dates("a: {0}\t{1}\n".format(s['date_added'],okay))
            item['properties']['datePublished'] = [s['date_added']]

        if not okay:
            return None

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

        try:
            self.filter_from = conf["harvest_from"]
        except:
            self.filter_from = None

        try:
            self.filter_until = conf["harvest_until"]
        except:
            self.filter_until = None

        self.endpoint = _ka_api_commoncore
        # self.endpoint = "file:/Users/jklo/projects/lr/workspace/DataPump/commoncore"
        pass

    def fetchEarliestDatestamp(self):
        return None

    def fetchRecords(self):

        tmp = tempfile.NamedTemporaryFile(prefix="khan_tmp_", dir=".")
        try:
            log.error("Temporary Fetch File: %s"%tmp.name)
            res = urllib2.urlopen(self.endpoint)
            res_line = res.read(256)
            while res_line:
                tmp.write(res_line)
                res_line = res.read(256)

            tmp.seek(0)

            standards = ijson.items(tmp, 'item')

            for std in standards:
                if ('videos' in std and len(std['videos']) > 0) or ('exercises' in std and len(std['exercises']) > 0):
                    for res in make_schema_org(std, self.filter_from, self.filter_until):
                        yield [ res ]
        finally:
            try:
                tmp.close()
            except:
                pass
  

    def tostring(self, rec):
        return json.dumps(rec).encode('utf-8')

    def load(self, f):
        return json.load(f)



if __name__ == '__main__':
    import argparse
    
    global _log_dates

    
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="test harvest interface for khan academy content")
    parser.add_argument('--from', dest="harvest_from", default=None, help="harvest_from date. ISO8601 format. YYYY-MM-DDTHH:mm:SSZ")
    parser.add_argument('--until', dest="harvest_until", default=None, help="harvest_until date. non-inclusive. ISO8601 format. YYYY-MM-DDTHH:mm:SSZ")
    parser.add_argument('--log-dates', dest="log_dates", default=False, type=bool, help="create a date log file for tracing.")
    args = parser.parse_args()

    if args.log_dates:
        _log_dates = open('./log_dates.log', 'w')

    f = Fetcher(conf={"harvest_from":args.harvest_from, "harvest_until": args.harvest_until})

    print '['
    for i, r in enumerate(f.fetchRecords()):
        if i > 0:
            print ','
        print f.tostring(r)
        sys.stdout.flush()
    print ']'

#    print '[\n{0}\n]'.format("\n,\n".join(map(lambda x: f.tostring(x), list(f.fetchRecords()))))

    
  
