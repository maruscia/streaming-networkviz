__author__ = 'maru'
import optparse
import re
from email.utils import parsedate
import json
import datetime
import tweepy




final_date = None

def parseOptions():
    parser = optparse.OptionParser()
    parser.add_option("-l", "--log", type="string", dest="log", help="Log file of collected streaming data", default='undefined')
    (options, _) = parser.parse_args()
    if options.log == 'undefined':
        parser.error("Log file is mandatory")
    return options




class StreamingListener(tweepy.StreamListener):
    

    def __init__(self, trending_date, *args, **kwargs):
        tweepy.StreamListener.__init__(self, *args, **kwargs)
        self.known_users={}
        self.seen_edges={}
        self.DATE_FIELD =0
        self.FOLLOWERS_FIELD =1
        self.TWEETS_FIELD=2
        self.RETWEETED_FIELD=3
        self.SKIPPING=0
        self.TRENDING_DATE = trending_date
        self.TRENDING_TIMESTAMP = None

        self.final_date=None
        self.edge_mapping = {}
        self.next_id=1
        self.last_tweet_hash = None
        self.timestamp=0


    def get_users(self):
        return self.known_users
    def get_edges(self):
        return self.seen_edges

    def get_final_date(self):
        return self.final_date

    def on_status(self,status):
        #print(status.text)
        h = hash(str(status))
        if self.last_tweet_hash is None:
            self.last_tweet_hash = h
        elif h == self.last_tweet_hash: #skip if near duplicates
            print("DUP!")
            return
        self.last_tweet_hash=h
        date = status.created_at


        #FIXME: add mentions
        rt = re.search('(?<=RT\s@)\w+', status.text)
        if rt:
            source_user = rt.group(0).lower()
            target_user = status.user.screen_name
            try:
                source_followers=status.retweeted_status.user.followers_count
            except AttributeError: #this guy doesn't know how to retweet properly..
                source_followers=0
            target_followers = status.user.followers_count
            id = status.id
            text = status.text

            tweet = (id, source_user, target_user, text, date,source_followers,target_followers)


        #retweet unmatched: tweet!
        else:
            id = status.id
            text = status.text
            followers = status.user.followers_count

            tweet = (id, status.user.screen_name,None, text, date,followers,None)

        self.handle_tweet(tweet)

    def handle_tweet(self,tweet):

        (id, source, target, text, date,followers1,followers2) = tweet

        source=str(source).lower()
        if target is not None:
            target=str(target).lower()

        if id is None:
            return

        #print(text + "-" + str(date))

        r=84./255.
        g=148./255.
        b=183./255.
        size = 5

        seconddate = "VOID"

        utcdate = "T".join(str(date).split(" "))
        utcdate2 = seconddate
        parsed_date = datetime.datetime.strptime(utcdate,"%Y-%m-%dT%H:%M:%S")


        if self.final_date is not None and parsed_date < datetime.datetime.strptime(self.final_date,"%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=-1):
            print("Possible duplicate detected.")
            #print(str(parsed_date))
            #print(str(self.final_date))
            return

        #Skip if skipping tweets prior to trending
        if parsed_date < self.TRENDING_DATE  and self.SKIPPING:
            return
        elif parsed_date >= self.TRENDING_DATE and self.TRENDING_TIMESTAMP is None:
            self.TRENDING_TIMESTAMP = self.timestamp+1

        self.final_date=  str(parsed_date + datetime.timedelta(minutes=1))

        if source not in self.known_users:
                                    #date,followers,tweets,retweeted

            self.known_users[source] = [utcdate,followers1,0,0,self.timestamp+1]


        #RETWEET CASE
        if target:
            if target not in self.known_users:
                self.timestamp+=1
                size =5
                self.known_users[target] = [utcdate,followers2,1,0,self.timestamp+1]

            else:
                #add +1 tweet to target
                self.known_users[target][self.TWEETS_FIELD]+=1

            #add +1 retweeted to source
            self.known_users[source][self.RETWEETED_FIELD]+=1

            edge_id=source+"_%%_"+target


            try:
                last_date1,last_date2,w = self.seen_edges[edge_id].pop()
                if utcdate == last_date1:
                    utcdate=  str(datetime.datetime.strptime(utcdate,"%Y-%m-%dT%H:%M:%S") + datetime.timedelta(seconds=1))
                self.seen_edges[edge_id].append([last_date1,utcdate,w]) #save previous last element
                w+=1
                if len(self.seen_edges[edge_id])<99999:
                    self.seen_edges[edge_id].append([utcdate,utcdate2,w])
                #print(self.seen_edges[edge_id])
            except KeyError:
                #new edge: update mapping
                self.edge_mapping[self.next_id]=edge_id
                self.next_id+=1
                w=1.0
                #<[2010.0, 2012.0, 3.0); [2012.0, 2014.0, 3.0)>
                this_weight = [utcdate,utcdate2,w]
                self.seen_edges[edge_id] = [this_weight]


        else:
            #single tweet
            self.known_users[source][self.TWEETS_FIELD]+=1

        self.timestamp+=1
    def finalize_edges(self,out):

        out.write("<edges>\n")
        i =0
        num_edges = len(self.seen_edges)
        for id in range(1,num_edges+1) :
            i+=1
            #if i>2000:
            #    break
            edge_id = self.edge_mapping[id]
            source,target =edge_id.split("_%%_")
            #closing last date

            self.seen_edges[edge_id][-1][1]=final_date
            seen_start = self.seen_edges[edge_id][0][0]
            edge_weight = self.seen_edges[edge_id][-1][2]
            out.write("<edge id=\""+str(id)+"\" label=\""+str(edge_id)+"\" source=\""+str(source)+"\" target=\""+str(target)+"\" start=\""+str(seen_start)+"\">\n<attvalues>")


            for l in self.seen_edges[edge_id]:
                start,end,w = l

                #<attvalue for=”2” value=”2” start=”2009−03−01” end=”2009−03−10”/>
                out.write("<attvalue for =\"weight\" value=\""+str(float(w))+"\" start=\""+start+"\" end=\""+end+"\"/>")

            out.write("</attvalues></edge>\n")
        out.write("</edges>\n</graph>\n</gexf>")



    def write_nodes_to_file(self,out):
        for user in self.known_users:
            date,followers,tweets,retweeted,timestamp = known_users[user]

            #save timestamp as difference with the trending timestamp (for ranking purposes)
            timestamp = (-1 * self.TRENDING_TIMESTAMP) + timestamp

            out.write("<node id=\""+ str(user)+ "\" label=\""+str(user)+"\" start=\""+str(date)+"\">\n<attvalues>" \
                    "<attvalue for =\"timestamp\" value=\""+str(float(timestamp))+"\"/>\n<attvalue for =\"followers\" value=\""+str(float(followers))+"\"/>\n"
                    "<attvalue for =\"tot_tweets\" value=\""+str(float(tweets))+"\"/><attvalue for =\"tot_retweets\" value=\""+str(float(retweeted))+"\"/>\n"
                    "</attvalues>\n</node>\n")
        out.write("</nodes>\n")




if __name__ == '__main__':
    options = parseOptions()
    log = open(options.log+".txt","r")
    out = open(options.log+".gexf",'w')

    for line in open(options.log+"_trending.txt","r"):
        trending_date = datetime.datetime.strptime(line,"%Y-%m-%dT%H:%M:%S")


    out.write("<gexf xmlns=\"http://www.gexf.net/1.2draft\" xmlns:viz=\"http://www.gexf.net/1.2draft/viz\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" version=\"1.2\" " \
	          "xsi:schemaLocation=\"http://www.gexf.net/1.2draft http://www.gexf.net/1.2draft/gexf.xsd\">\n<graph mode=\"dynamic\" timeformat=\"datetime\" defaultedgetype=\"directed\">\n<attributes class=\"node\">\n<attribute id=\"timestamp\" title=\"Timestamp\" type=\"double\"/>\n<attribute id=\"tot_tweets\" title=\"TotalTweets\" type=\"double\"/>" \
	          "<attribute id=\"tot_retweets\" title=\"TotalRetweets\" type=\"double\"/><attribute id=\"followers\" title=\"Followers\" type=\"double\"/></attributes>\n<attributes class=\"edge\" mode=\"dynamic\">"
	          "\n<attribute id=\"weight\" title=\"Weight\" type=\"float\"/>\n</attributes>\n<nodes>")#<attribute id=\"mentions\" title=\"Mentions\" type=\"integer\"/>


    listener = StreamingListener(trending_date)
    for line in log:
        listener.on_data(line)

    final_date = listener.get_final_date()
    seen_edges = listener.get_edges()
    known_users = listener.get_users()

    listener.write_nodes_to_file(out)
    listener.finalize_edges(out)
    out.close()