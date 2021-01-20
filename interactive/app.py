from starlette.applications import Starlette
from starlette.responses import UJSONResponse
import uvicorn

import tweepy
import time
import os
import aiohttp
import asyncio
from sqlalchemy import engine, create_engine, MetaData, Table
from sqlalchemy.sql.expression import func, select
from sqlalchemy.sql import text

# Twitter app configuration information: required
CONSUMER_KEY = os.environ.get('CONSUMER_KEY')
CONSUMER_SECRET = os.environ.get('CONSUMER_SECRET')
ACCESS_KEY = os.environ.get('ACCESS_KEY')
ACCESS_SECRET = os.environ.get('ACCESS_SECRET')

ACCOUNT = os.environ.get("ACCOUNT")
DB_USER = os.environ.get("DB_USER")
DB_PASS = os.environ.get("DB_PASS")
DB_NAME = os.environ.get("DB_NAME")
CONNECTION_NAME = os.environ.get("CONNECTION_NAME")

# todo include this for server
assert all([CONSUMER_KEY, CONSUMER_SECRET, ACCESS_KEY, ACCESS_SECRET]
           ), "Not all Twitter app config tokens have been specified."

INVALID_FILE = 'file_is_invalid'
NO_FILE = 'file_does_not_exist'
FILE_OF_PROFANE_PHRASES = 'profanities_en.txt'
SERVER_ERROR_STATUS = 'server_error_status: '
CLOUD_RUN_URL = "https://url.to.cloud.run"

USER_NAME = os.environ.get('TWITTER_BOT_ACCOUNT')

SERVER_PROBLEM = "sorry, we're having some problems with your request, please try again later"
PROFANITY_PROBLEM = 'sorry, your request for an object breached our list of profanities.  ' \
                    'Please DM us if you think this was a mistake, otherwise please try again'
FORMAT_PROBLEM = 'sorry, your request does not match our format. Please start your tweet with ' \
                 'our username, do not mention other users in your tweet, and do not use hashtags either.'

auth = tweepy.OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
auth.set_access_token(ACCESS_KEY, ACCESS_SECRET)

api = tweepy.API(auth)

profanity_list = []
db_results = []

MENTION_ID = 'mention_id'
TIMESTAMP = 'timestamp'
OUR_STATUS = 'our_status'
OUR_RESPONSE = 'our_response'
ORIGINAL_REQUEST = 'original_request'
TWITTER_URL = 'twitter_url'


def build_profanity_list():
    # builds a list of profanities taken from:
    # https://github.com/LDNOOBW/List-of-Dirty-Naughty-Obscene-and-Otherwise-Bad-Words
    file_read = open(FILE_OF_PROFANE_PHRASES, 'r')
    for line in file_read:
        inner_list = [elt.strip() for elt in line.split(',')]
        profanity_list.append(inner_list)


def is_profane(mention):
    # check to make sure we've built our list of profanities from our file

    if len(profanity_list) < 1:
        build_profanity_list()

    message_from_user = mention.full_text
    # check to see if anything in the user's request could be considered 'profane'
    for profanity in profanity_list:
        if profanity[0].lower() in message_from_user.lower():
            # print('found profanity: ' + profanity[0] + ' in: ' + message_from_user)
            return True
    return False


def send_reply_to_mention(reply, mention):
    # TODO: handle errors! This can crash if it tries to send somethign we have sent before (which 
    # shouldn't happen).  Try catching it.
    api.update_status('@' + mention.user.screen_name + " " + reply, mention.id)
    our_status = 'ERROR'
    if reply == SERVER_PROBLEM:
        our_status = 'ERROR 1'
        #print('writing to database server response error: ', str(mention.id))
    elif reply == PROFANITY_PROBLEM:
        our_status = 'ERROR 2'
        #print('writing to database profanity warning sent to tweet: ', str(mention.id))
    elif reply == FORMAT_PROBLEM:
        our_status = 'ERROR 3'
        #print('writing to database format warning sent to tweet: ', str(mention.id))
    else:
        our_status = 'SUCCESS'
        #print('writing to database successful tweet sent to', str(mention.id))

    # store the data for insertion into the database logs
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S+00", time.gmtime())
    t_url = f"https://twitter.com/{mention.user.screen_name}/status/{mention.id_str}"
    db_results.append({MENTION_ID: mention.id, OUR_STATUS: our_status, TIMESTAMP: timestamp,
                       OUR_RESPONSE: reply, ORIGINAL_REQUEST: mention.full_text, TWITTER_URL: t_url})


def is_correct_format(mention):
    # to be correct format first mention should be our username and there should not be any
    # other user names or hashtags
    # TODO clean this up, it is pretty ugly
    message_from_user = mention.full_text
    if message_from_user[0:len(USER_NAME)].lower() == USER_NAME.lower():
        # message contains our user name as first part
        if '@' in message_from_user[len(USER_NAME):len(message_from_user)] or \
                '#' in message_from_user[len(USER_NAME):len(message_from_user)]:
            # message contains other users or hashtags
            # print('format issue other: ' + message_from_user)
            return False
        return True
    return False


def is_valid_tweet(tweet, mention):
    # TODO return error codes
    # tweet must be < 240 characters and have returned from the API in full and the should be different to the request
    original_request = mention.full_text[len(USER_NAME):len(mention.full_text)]
    if len(tweet) > 240:
        # it is too long
        # print('tweet to long: ' + tweet)
        return False
    if '<' in tweet:
        # if it contains a < we can assume it has part of <|endoftext|> in its response
        # print('tweet contains <: ' + tweet)
        return False
    if len(tweet.lower().rstrip()) < len(original_request.lower()) + 2:
        # Iv'e noticed some tweets just come back with a . on the end of them, this should account for those
        # print('response from API was unchanged from original request: ' + tweet)
        return False
    if tweet[-1] == '\n' and tweet[-2] == '\n':
        # print('tweet has newlines!: ' + tweet)
        # if it has passed the two previous tests and ends with two newline characters we can assume it passes
        return True
    # print('problem with tweet: ' + tweet)
    return False


async def get_json(m, session):
    object_request = m.full_text[len(USER_NAME):len(m.full_text)]
    # print("request: " + object_request + " for id: " + str(m.id))

    post_dict = {
        "length": 75,
        "temperature": 0.7,
        "prefix": object_request,
        "truncate": "<|endoftext|>"
    }

    async with session.post(CLOUD_RUN_URL, json=post_dict) as response:
        if response.status == 200:
            return await response.json()
        else:
            return SERVER_ERROR_STATUS + str(response.status)


async def fetch(count, mention, session):
    # TODO refactor this
    response = '<error>'
    data = await get_json(mention, session)
    if 'text' in data:
        response = data['text']
    else:
        print('received server error: ' + data)
    # print('received data: ' + response)
    attempts = 1
    while attempts < 4:
        if is_valid_tweet(response, mention):
            # post the validated response to twitter here.
            send_reply_to_mention(response.rstrip(), mention)
            break
        elif attempts == 3:
            send_reply_to_mention(SERVER_PROBLEM, mention)
            # TODO: notify someone of this error (email?)
        else:
            # print('attempting to obtain more json')
            data = await get_json(mention, session)
            if 'text' in data:
                response = data['text']
            else:
                response = '<error>'
                print('received server error: ' + data)
        attempts += 1

    return 'DONE:', str(count) + '\n'


async def reply():
    # print("Replying...", flush=True)

    # connect to our database
    db = create_engine(
        engine.url.URL(
            drivername='postgres+pg8000',
            username=DB_USER,
            password=DB_PASS,
            database=DB_NAME,
            query={
                'unix_sock': '/cloudsql/{}/.s.PGSQL.5432'.format(
                    CONNECTION_NAME)
            }
        ),
        pool_size=1
    )

    # obtain the most recent mention from our db 
    with db.connect() as conn:
        s = text(
            "Select MAX(mention_id) as mention_id FROM requests"
        )

        max_id = conn.execute(s).fetchone()

    # Get the list of recent mentions not ALL mentions
    if max_id[0] is None:
        # when the table is empty the query returns a tuple with None in the first spot
        mentions = api.mentions_timeline(tweet_mode='extended')
    else:
        mentions = api.mentions_timeline(max_id[0], tweet_mode='extended')

    if len(mentions) < 1:
        return ('no new mentions')

    print('received ' + str(len(mentions)) + ' new mentions.')
    most_recent_id = mentions[0].id
    
    # update our db with the most recent mention
    metadata = MetaData()
    requests_table = Table('requests', metadata, autoload=True, autoload_with=db)

    with db.connect() as conn:
        ins = requests_table.insert().values(mention_id=most_recent_id)
        conn.execute(ins)

    # print(mentions)

    # this is used to keep the session running and makes the requests take place in
    # parallel rather than series.
    responses = []

    async with aiohttp.ClientSession() as session:
        i = 0
        while i < len(mentions):
            # confirm request is valid and not profane
            if is_profane(mentions[i]):
                # let the user know their request was considered profane
                send_reply_to_mention(PROFANITY_PROBLEM, mentions[i])
                break
            # confirm request is in valid format
            elif not is_correct_format(mentions[i]):
                # let the user know their request did not meet our format requirements
                send_reply_to_mention(FORMAT_PROBLEM, mentions[i])
                break
            else:
                # attempt to get a response
                response = asyncio.ensure_future(fetch(i, mentions[i], session))
                responses.append(response)
            i += 1

        all_responses = await asyncio.gather(*responses)
        print('made ' + str(len(responses)) + ' responses.')

    # print('sending results to the database')
    metadata = MetaData()
    interactions = Table('interactions', metadata, autoload=True, autoload_with=db)

    global db_results
    db_entry = db_results
    db_results = []

    with db.connect() as conn:
        # db_results is a pretty ugly global variable hack and could probably be handled more elegantly
        for entry in db_entry:
            q_insert = (
                interactions
                .insert()
                .values(mention_id=str(entry[MENTION_ID]),
                        tweet_timestamp=entry[TIMESTAMP],
                        our_status=entry[OUR_STATUS],
                        tweet=entry[OUR_RESPONSE],
                        original_request=entry[ORIGINAL_REQUEST],
                        tweet_url=entry[TWITTER_URL])
            )

            conn.execute(q_insert)

    db.dispose()

    return 'sent ' + str(len(responses)) + ' responses.'

app = Starlette(debug=False)

# Needed to avoid cross-domain issues
response_header = {
    'Access-Control-Allow-Origin': '*'
}

@app.route('/')
async def start(request):
    code = await reply()
    return UJSONResponse({'text': code},
                         headers=response_header)

if __name__ == '__main__':
    uvicorn.run(app, host='0.0.0.0', port=int(os.environ.get('PORT', 8080)))
