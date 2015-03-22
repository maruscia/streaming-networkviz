#!/usr/bin/python
# coding: utf-8
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

'''
This script starts an HTTP server in order to replay a stream of json data
collected from the Twitter Streaming API.
It shows the Twitter data in Graph Streaming format,
with the users as nodes and retweets as edges.


To connect to the server with Gephi, you must install Gephi with
the Graph Streaming plugin.

1. Start Gephi
3. Go to the tab Streaming,right-click on Client and click on "Connect to Stream"
2. Start this script in order to start the server
4. Go to Gephi and enter the Source URL http://localhost:8181/?q=twitter (or other keywords
    used during the data collection) and click OK

The nodes and edges start to appear in the graph visualization. You can run
the Force Atlas layout in order to get a better layout.

Usage: server.py [options]

Options:
  -h, --help            show this help message and exit
  -l LOG, --log=LOG     Log file of collected streaming data
  -t tw, --timewarp=tw  Time warping factor, used to accelerate or slow down the replay
  -d s, --delay=s       Starting delay in seconds
@author maru
'''

from http.server import BaseHTTPRequestHandler, HTTPServer
import urllib.parse
import tweepy
import re
import datetime
try:
    import simplejson
except ImportError:
    try:
        import json as simplejson
    except:
        raise "Requires either simplejson or Python 2.6!"
import threading
import queue
import socket
from socketserver import ThreadingMixIn
import optparse
import sys
import time

active_queues = []

class StreamingListener(tweepy.StreamListener):

    def __init__(self, timewarp, *args, **kwargs):
        tweepy.StreamListener.__init__(self, *args, **kwargs)
        self.known_users = {}
        self.before = None
        self.timewarp = timewarp

    def on_status(self, status):
        #print(status.text)

        date = status.created_at
        print(str(date) + "-" +str(status.user.screen_name) + " - " +str(status.text))
        # if not self.before:
        #     self.before = date
        #
        # if date > self.before:
        #     diff = date - self.before
        #
        #     sleeptime = diff.seconds + diff.microseconds*10e-6
        #     if sleeptime > 0:
        #         time.sleep(sleeptime*self.timewarp)
        #     self.before = date
        #FIXME: add mentions  && single tweets ?
        m = re.search('(?<=RT\s@)\w+', status.text)
        if m:
            source_user = m.group(0).lower()
            target_user = status.user.screen_name
            source_followers=status.retweeted_status.user.followers_count
            target_followers = status.user.followers_count
            id = status.id
            text = status.text

            dispatch_event((id, source_user, target_user, text, date,source_followers,target_followers))
            #check retweeted_status.followers_count
        #retweet unmatched: tweet!
        else:
            id = status.id
            text = status.text
            followers = status.user.followers_count

            dispatch_event((id, status.user.screen_name,None, text, date,followers,None))

def dispatch_event(e):
    for q in active_queues:
        q.put(e)

class RequestHandler(BaseHTTPRequestHandler):

    def __init__(self, *args, **kwargs):
        BaseHTTPRequestHandler.__init__(self, *args, **kwargs)

    def handle_tweets(self,terms):
        while True:

            self.timestamp+=1

            (id, source, target, text, date,followers1,followers2) = self.queue.get()

            source=str(source).lower()
            if target is not None:
                target=str(target).lower()

            if id == None:
                break

            # found = False
            # for term in terms:
            #     if re.search(term, text.lower()):
            #         found = True
            # if not found:
            #     continue


            r=84./255.
            g=148./255.
            b=183./255.
            size = 5
            seconddate = date + datetime.timedelta(days=5)

            utcdate = str(date).split(" ")
            utcdate2 = str(seconddate).split(" ")
            try:

                if source not in self.known_users:
                    if not target: #only tweet
                        r=0.0
                        g=1.0
                        b=0.0
                    if len(self.known_users) == 0: #first user
                        size=15
                    if len(self.known_users)<10:
                        r=255./255.
                        g=0.0
                        b=0.0
                    self.known_users[source] = source
                    event = simplejson.dumps({'an':{source:{'label':source, 'size':size, 'r':r, 'g':g, 'b':b,'followers':followers1, 'timestamp':self.timestamp, 'Time Interval':"<["+str(utcdate[0])+"T"+str(utcdate[1])+","+str(utcdate2[0])+"T"+str(utcdate2[1])+"]>"}}})
                    self.wfile.write(str.encode(event))
                    self.wfile.write(str.encode('\r\n\r\n'))

                #RETWEET CASE
                if target:
                    if target not in self.known_users:
                        if len(self.known_users)<10:
                            r=255./255.
                            g=0.0
                            b=0.0
                        size =5
                        self.known_users[target] = target
                        event = simplejson.dumps({'an':{target:{'label':target, 'size':size, 'r':r, 'g':g, 'b':b,'followers':followers2,'timestamp':self.timestamp,'Time Interval':"<["+str(utcdate[0])+"T"+str(utcdate[1])+","+str(utcdate2[0])+"T"+str(utcdate2[1])+"]>"}}})
                        self.wfile.write(str.encode(event))
                        self.wfile.write(str.encode('\r\n'))


                    id=source+"__"+target

                    try:
                        w = self.seen_edges[id]+1.0
                        event = simplejson.dumps({'ce':{id:{'weight':w}}})
                    except KeyError:
                        w=1.0
                        event = simplejson.dumps({'ae':{id:{'source':source, 'target':target, 'directed':True, 'weight':w, 'date':str(date)}}})

                    self.seen_edges[id]=w
                    #print(w)

                self.wfile.write(str.encode(event))
                self.wfile.write(str.encode('\r\n'))

            except socket.error:
                print("Connection closed")

    def do_POST(self):
        pass

    def do_GET(self):

        param_str = urllib.parse.urlparse(self.path).query
        parameters = urllib.parse.parse_qs(param_str, keep_blank_values=False)
        if "q" not in parameters:
            return

        q = parameters["q"][0]
        terms = q.split(",")

        print("Request for tweets, query '%s'"%q)

        self.queue = queue.Queue()
        self.known_users = {}
        self.seen_edges = {}
        self.timestamp = 0
        active_queues.append(self.queue)

        self.wfile.write(str.encode('\r\n'))
        self.handle_tweets(terms)
        active_queues.remove(self.queue)
        return

class Player(threading.Thread):
    def __init__(self, options, server):
        self.options = options
        self.server = server
        threading.Thread.__init__(self)

    def run(self):
        print("Waiting %s seconds before start streaming" % self.options.delay)
        time.sleep(self.options.delay)

        print("Streaming retweets for file '%s'"%self.options.log)
        listener = StreamingListener(self.options.timewarp)
        f = open(self.options.log)

        line = f.readline()
        while line != '':
            listener.on_data(line)
            line = f.readline()

        print("Stream finished")
        dispatch_event((None, None, None, None, None, None, None))
        self.server.shutdown()

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    """Handle requests in a separate thread."""

    def start(self):
        self.serve_forever()

    def stop(self):
        self.socket.close()

def parseOptions():
    parser = optparse.OptionParser()
    parser.add_option("-l", "--log", type="string", dest="log", help="Log file of collected streaming data", default='undefined')
    parser.add_option("-t", "--timewarp", type="float", dest="timewarp", help="Time warping factor, used to accelerate or slow down the replay", default='1.0')
    parser.add_option("-d", "--delay", type="int", dest="delay", help="Starting delay in seconds", default='0')
    parser.add_option("-s", "--serverport", type="int", dest="serverport", help="HTTP server port", default=8181)
    (options, _) = parser.parse_args()
    if options.log == 'undefined':
        parser.error("Log file is mandatory")
    return options

def main():
    options = parseOptions()

    try:
        server = ThreadedHTTPServer(('', options.serverport), RequestHandler)

        player = Player(options, server)
        player.setDaemon(True)
        player.start()

        print('Test server running...')
        server.start()
    except KeyboardInterrupt:
        print('Stopping server...')
        server.stop()
        dispatch_event((None, None, None, None, None,None,None))
        sys.exit(0)

if __name__ == '__main__':
    main()
