from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
from kafka import KafkaProducer
import pprint



 
#consumer key, consumer secret, access token, access secret.
ckey="dSz5TgR3oRR0aAcx8sLg1dWJ7"
csecret="B7RaKQA14bRVp9yViQrQNhHU7cZQP7cyhJdRffnt7TPWHdYtue"
atoken="1397178062647693321-98deqStu35EJo7hsSE9PM7rf6sVaZZ"
asecret="lLg9liDgsuOa3Us1Y3GeBefZRecEzwUUxnrq3FTMHOdJ1"
 
class listener(StreamListener):
 
    def on_data(self, data):
        all_data = json.loads(data)
        data = str(all_data['in_reply_to_screen_name']).encode("ascii", "ignore").decode()
 
        if(data !="None"):
            producer.send('new',data)
            producer.flush()
 
            pprint.pprint(data)
        #pprint.pprint(all_data)
        return True
 
    def on_error(self, status):
        print(status)
        
producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'),bootstrap_servers=['localhost:9092'])
 
auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)
 
twitterStream = Stream(auth, listener())
twitterStream.filter(track=["आज_भारत_बंद_है", "भारत_खुला_है","BharatBandh"])