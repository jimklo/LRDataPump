'''
Created on Oct 25, 2011

@author: jklo
'''
import threading
import tempfile
from lxml import etree
import Queue
import os
import logging

log = logging.getLogger(__name__)

class RepoExtractor(threading.Thread):
    
    def __init__(self, run, queue=Queue.Queue(), workdir="."):
        threading.Thread.__init__(self)
        self.runner = run
        self.queue = queue
        self.workdir = tempfile.mkdtemp(dir=workdir)
        self.done = False
        
    def run(self):
        try:
            for (index, recset) in enumerate(self.runner.fetcher.fetchRecords()):
                log.info("Writing cache #{0} : {1} files".format(index, len(recset)))
                for (idx2, rec) in enumerate(recset):
                    fname = os.path.join(self.workdir, "cache-{0}-{1}.xml".format(index,idx2))
                    with open(fname, "w") as f:
                        f.write(etree.tostring(rec).encode("utf-8"))
                    self.queue.put(fname)
                    
        except:
            log.exception("Problem writing files")
        finally:
            self.done = True
            

class LRPublisher(threading.Thread):
    def __init__(self, run, queue=Queue.Queue()):
        threading.Thread.__init__(self)
        self.runner = run
        self.queue = queue
        
    
    def run(self):
        while True:
            try:
                fname = self.queue.get()
                with open(fname) as x:
                    rec = etree.parse(x)
                    if self.runner.transformer is not None:
                        (repo_id, doc) = self.runner.transformer.format(rec)
                        seen = self.runner.couch.have_i_seen(repo_id)
                        if not seen or seen["published"] == False:
                            doc = self.runner.sign(doc)
                            if (doc != None and repo_id != None):
                                self.runner.docList[repo_id] = doc
                    self.runner.publishToNode()
                os.unlink(fname)
            except:
                log.exception("Stopping")
            finally:
                self.queue.task_done()
   
                