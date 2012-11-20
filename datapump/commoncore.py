import csv, re

CURRENT = "Current URL"



class CommonCore():
    def __init__(self, mapfile="./datapump/E0607_ccss_identifiers_modified.csv"):
        self.mapfile = mapfile

        self.current_index = {}
        self.current_hash = {}
        self.all = []

        with open(self.mapfile, 'rU') as csvfile:
            reader = csv.DictReader(csvfile)

            for row in reader:
                if CURRENT in row and row[CURRENT] != None and row[CURRENT].strip() != "":
                    self.current_index[row[CURRENT].strip()] = row
                    self.current_index[row[CURRENT].strip().lower()] = row
                    match = re.search("#[^#]+$", row[CURRENT].strip())
                    if match != None:
                        self.current_hash[match.group(0)] = row
                    self.all.append(row)

    def getFromCurrent(self, current_url):
        if current_url in self.current_index:
            return self.current_index[current_url]
        else:
            return None


    def getFromCurrentHash(self, current_url):
        match = re.search("#([^#]+)$", current_url)
        if match != None and match.group(0) in self.current_hash:
            return self.current_hash[match.group(0)]
        elif match != None and "#-{0}".format(match.group(1)) in self.current_hash:
            return self.current_hash["#-{0}".format(match.group(1))]
        else:
            return None

ccss = CommonCore()

if __name__ == '__main__':
    import json

    cc = CommonCore()

    for i in cc.current_index.keys():
        print i

    print json.dumps(cc.getFromCurrent('http://www.corestandards.org/the-standards/mathematics/kindergarten/counting-and-cardinality/#k-cc-6'))


