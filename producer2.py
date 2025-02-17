from tweepy import Stream
from tweepy import OAuthHandler
from tweepy.streaming import StreamListener
import time
import json
from kafka import KafkaProducer




#consumer key, consumer secret, access token, access secret.
ckey="dSz5TgR3oRR0aAcx8sLg1dWJ7"
csecret="B7RaKQA14bRVp9yViQrQNhHU7cZQP7cyhJdRffnt7TPWHdYtue"
atoken="1397178062647693321-98deqStu35EJo7hsSE9PM7rf6sVaZZ"
asecret="lLg9liDgsuOa3Us1Y3GeBefZRecEzwUUxnrq3FTMHOdJ1"


class listener(StreamListener):

    def on_data(self, data):
        all_data = json.loads(data)
#       producer.send('new',all_data)
#       producer.flush()
        
       
        print(all_data)
        
        return True

    def on_error(self, status):
        print(status)
        
#producer = KafkaProducer(value_serializer=lambda m: json.dumps(m).encode('utf-8'),bootstrap_servers=['localhost:9092'])


auth = OAuthHandler(ckey, csecret)
auth.set_access_token(atoken, asecret)

twitterStream = Stream(auth, listener())
twitterStream.filter(track=["car"])
