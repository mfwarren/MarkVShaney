#!/usr/bin/env python

import StringIO
import random
import time
import sys
from textwrap import TextWrapper
from optparse import OptionParser

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Table, Column, Integer, String, MetaData, Date, DateTime, Float
from sqlalchemy.schema import UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base

import tweepy
import twitter

CONNSTRING='sqlite:///MarkVShaney.sqlite'

TRAINING_SEARCH_KEYWORDS = ['senate', 'government', 'federal', 'usda', 'ftc', 'usaid', 'nasa', 'noaa', 'usajobs' 'congress']

#for connecting to hose
TWITTER_USER = "YOUR USER"
TWITTER_PW = "YOUR PW"

#for tweeting
TWITTER_CONSUMER_KEY = 'YOUR KEY'
TWITTER_CONSUMER_SECRET = 'YOUR KEY'
TWITTER_ACCESS_TOKEN_KEY = 'YOUR KEY'
TWITTER_ACCESS_TOKEN_SECRET = 'YOUR KEY'


#command line options
parser = OptionParser()
parser.add_option('-l', '--listen', action='store_true', dest='listen', default=False, help='listen for tweets and build database')
(options, args) = parser.parse_args()


Base = declarative_base()
class StreamWatcherListener(tweepy.StreamListener):

    status_wrapper = TextWrapper(width=60, initial_indent='    ', subsequent_indent='    ')
    engine = create_engine(CONNSTRING, echo=False)

    def __init__(self):
        self.metadata = Base.metadata
        self.metadata.create_all(self.engine)
        super(StreamWatcherListener, self).__init__()

    def on_status(self, status):
        try:
            if status.author.lang == 'en' and len(status.text.strip().split(' ')) > 15:
                #print self.status_wrapper.fill(status.text)
                #print '\n %s  %s  via %s\n' % (status.author.screen_name, status.created_at, status.source)
                Session = sessionmaker(bind=self.engine)
                session = Session()
                tweet = Tweet(status.created_at, status.source, status.text, status.author.screen_name)
                session.add(tweet)
                session.commit()
        except Exception as ex:
            # Catch any unicode errors while printing to console
            print ex.args[0]
            # and just ignore them to avoid breaking application.
            pass

    def on_error(self, status_code):
        print 'An error has occured! Status code = %s' % status_code
        return True  # keep stream alive

    def on_timeout(self):
        print 'Snoozing Zzzzzz'

class Tweet(Base):
    """
    This defines to sqlachemey how to store tweets in the database.
    """
    __tablename__ = 'tweets'

    id = Column(Integer, primary_key=True)
    date = Column(Date)
    source = Column(String)
    text = Column(String)
    screen_name = Column(String)

    def __init__(self, date, source, text, screen_name):
        self.date = date
        self.text = text
        self.screen_name = screen_name
        self.source = source


    def __repr__(self):
        return "%s - %s" % (str(self.screen_name), str(self.text))


class Markov(object):
    def __init__(self, open_file):
        self.cache = {}
        self.open_file = open_file
        self.words = self.file_to_words()
        self.word_size = len(self.words)
        self.database()

    def file_to_words(self):
        self.open_file.seek(0)
        data = self.open_file.read()
        words = data.split()
        return words
    def triples(self):
        """ Generates triples from the given data string. So if our string were
                "What a lovely day", we'd generate (What, a, lovely) and then
                (a, lovely, day).
        """
        if len(self.words) < 3:
            return
        for i in range(len(self.words) - 2):
            yield (self.words[i], self.words[i+1], self.words[i+2])
    def database(self):
        for w1, w2, w3 in self.triples():
            key = (w1, w2)
            if key in self.cache:
                self.cache[key].append(w3)
            else:
                self.cache[key] = [w3]
    def generate_markov_text(self, size=25):
        seed = random.randint(0, self.word_size-3)
        seed_word, next_word = self.words[seed], self.words[seed+1]
        w1, w2 = seed_word, next_word
        gen_words = []
        for i in xrange(size):
            gen_words.append(w1)
            w1, w2 = w2, random.choice(self.cache[(w1, w2)])
        gen_words.append(w2)
        return ' '.join(gen_words)

def tweet(message):
    """
    posts tweet
    """
    api = twitter.Api(consumer_key=TWITTER_CONSUMER_KEY, consumer_secret=TWITTER_CONSUMER_SECRET, access_token_key=TWITTER_ACCESS_TOKEN_KEY, access_token_secret=TWITTER_ACCESS_TOKEN_SECRET)
    status = api.PostUpdate(message)
    print message

def train_markov():
    """Get the tweets from DB and push into markov for training
    """
    engine = create_engine(CONNSTRING, echo=False)

    metadata = Base.metadata
    metadata.create_all(engine)

    Session = sessionmaker(bind=engine)
    session = Session()
    tweets = session.query(Tweet).all()
    text = StringIO.StringIO()
    for tweet in tweets:
        try:
            text.write(' %s ' % tweet.text)
        except:
            pass
    mark = Markov(text)

    return mark


def main():
    auth = tweepy.auth.BasicAuthHandler(TWITTER_USER, TWITTER_PW)
    stream = tweepy.Stream(auth, StreamWatcherListener(), timeout=None)
    stream.filter(None, TRAINING_SEARCH_KEYWORDS)


if __name__ == '__main__':
    if options.listen:
        print "connecting to twitter hose"
        try:
            main()
        except KeyboardInterrupt:
            print '\nGoodbye!'
    else:
        mark = train_markov()
        message = mark.generate_markov_text()
        while len(message) > 140 or message.find("RT") >-1 :
            message = mark.generate_markov_text()

        tweet(message)
