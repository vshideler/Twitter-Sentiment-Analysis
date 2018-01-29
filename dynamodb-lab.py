###########################
# DynamoDB Tutorial
###########################

"""
$ pip install tweepy
$ pip install boto

"""

import boto.dynamodb2
from boto.dynamodb2.fields import HashKey, RangeKey, KeysOnlyIndex, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER

# make a connection to dynamodb
conn = boto.dynamodb2.connect_to_region(region_name='us-east-1', 
                                        aws_access_key_id='AKIAJTZHUI433MBIH46A', aws_secret_access_key='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')

##############################
# create a new tweets tables
##############################

# create a new dynamo table - example 2
tweets = Table.create('tweets', schema=[
    HashKey('id'),
], throughput={
    'read': 5,
    'write': 10,
},
    connection=conn
)


###########################
## Creating new item
###########################

# use put_item method
tweets = Table('tweets', connection=conn)
tweets.put_item(data={
    'id': '1111',
    'username': 'xxxx',
    'screen_name': 'yyyy',
    'tweet': 'Hello twitter!'
})


# construct an item and then save it
from boto.dynamodb2.items import Item
from boto.dynamodb2.table import Table
tweets = Table('tweets', connection=conn)
_item = Item(tweets,
               data={
                   'id': '2222',
                   'username': 'cccc',
                   'screen_name': 'dddd',
                   'tweet': 'test tweet',
               })

_item.save()



###########################
## Getting an item
###########################

from boto.dynamodb2.table import Table
tweets = Table('tweets', connection=conn)
_tweet = tweets.get_item(id='1111')
print type(_tweet)
print _tweet.keys()
for key in _tweet.keys():
    print key, _tweet[key]


###########################
## Updating an item
###########################
from boto.dynamodb2.table import Table
tweets = Table('tweets', connection=conn)


## Update and Save
_tweet = tweets.get_item(id='1111')
_tweet['tweet'] = 'Updating dynamodb item'
del _tweet['username']  # NOTE: doesn't save yet!
print tweets.get_item(id='1111').keys()

_tweet.save()

# full overwrite. If you can be confident your version of the data is the most correct, you can force an overwrite of the data.:
_tweet.save(overwrite=True)

## put_item overwrite
tweets.put_item(data={
                  'id': '2222',
                  'username': 'ssss'
               },
        overwrite=True)

tweets.get_item(id='2222').items()



###########################
## Deleting an item
###########################

# If you already have an Item instance, the easiest approach is just to call Item.delete

_tweet.delete()
tweets.get_item(id='1111').items()   # return item not found

# If you don't have an Item instance
from boto.dynamodb2.table import Table
tweets = Table('tweets')
tweets.delete_item(id='2222')


###########################
## Batch writing
###########################

# If you’re loading a lot of data at a time, making use of batch writing can both speed up the process &
# reduce the number of write requests made to the service.

# Batch writing involves wrapping the calls you want batched in a context manager. The context manager
# imitates the Table.put_item & Table.delete_item APIs. Getting & using the context manager looks like:

import time
from boto.dynamodb2.table import Table
tweets = Table('tweets')

with tweets.batch_write() as batch:
    batch.put_item(data={
        'id': '1111',
        'username': 'xxxx',
        'screen_name': 'yyyy',
        'tweet': 'yes yes',
    })
    batch.put_item(data={
        'id': '2222',
        'username': 'cccc',
        'screen_name': 'dddd',
        'tweet': 'no no',
    })


# Additionally, the context manager can only batch 25 items at a time for a request (this is a DynamoDB limitation).
# It is handled for you so you can keep writing additional items, but you should be aware that 100 put_item calls is
# 4 batch requests, not 1.



###########################
## Querying
###########################

# To cope with fetching many records, you can either perform a standard query,
# query via a local secondary index or scan the entire table.

# A standard query typically gets run against a hash+range key combination.
# Filter parameters are passed as kwargs & use a __ to separate the fieldname from the operator being used to
# filter the value.


from boto.dynamodb2.fields import HashKey, RangeKey, GlobalAllIndex
from boto.dynamodb2.table import Table
from boto.dynamodb2.types import NUMBER
import time

users = Table.create('users2', schema=[
    HashKey('account_type'),
    # defaults to STRING data_type
    RangeKey('last_name'),
], throughput={
    'read': 5,
    'write': 15,
}, global_indexes=[
    GlobalAllIndex('DateJoinedIndex', parts=[
        HashKey('account_type'),
        RangeKey('date_joined', data_type=NUMBER)
    ],
    throughput={
        'read': 1,
        'write': 1,
    })
],
    connection=conn
)

with users.batch_write() as batch:
     batch.put_item(data={
         'account_type': 'standard_user',
         'first_name': 'John',
         'last_name': 'Doe',
         'is_owner': True,
         'email': True,
         'date_joined': int(time.time()) - (60*60*2),
     })
     batch.put_item(data={
         'account_type': 'standard_user',
         'first_name': 'Jane',
         'last_name': 'Doering',
         'date_joined': int(time.time()) - 2,
     })
     batch.put_item(data={
         'account_type': 'standard_user',
         'first_name': 'Bob',
         'last_name': 'Doerr',
         'date_joined': int(time.time()) - (60*60*3),
     })
     batch.put_item(data={
         'account_type': 'super_user',
         'first_name': 'Alice',
         'last_name': 'Liddel',
         'is_owner': True,
         'email': True,
         'date_joined': int(time.time()) - 1,
     })


# When executing the query, you get an iterable back that contains your results.
# These results may be spread over multiple requests as DynamoDB paginates them.
# This is done transparently, but you should be aware it may take more than one request.


# QUERY: To run a query for last names starting with the letter “D”:

names_with_d = users.query_2(
     account_type__eq='standard_user',
     last_name__beginswith='D'
 )
for user in names_with_d:
    print user['first_name']


rev_with_d = users.query_2(
     account_type__eq='standard_user',
     last_name__beginswith='D',
     reverse=True,
     limit=2
 )

for user in rev_with_d:
     print user['first_name']


# QUERY: You can also run queries against the local secondary indexes
# Users within the last hour.
recent = users.query_2(
     account_type__eq='standard_user',
     date_joined__gte=time.time() - (60 * 60),
     index='DateJoinedIndex'
 )

for user in recent:
     print user['first_name']


all_users = users.query_2(
     account_type__eq='standard_user',
     date_joined__gte=0,
     index='DateJoinedIndex',
     max_page_size=10
 )

# Usage is the same, but now many smaller requests are done.
for user in all_users:
     print user['first_name']




###########################
## Batch Reading
###########################

# Similar to batch writing, batch reading can also help reduce the number of API
#  requests necessary to access a large number of items.

from boto.dynamodb2.table import Table
users = Table('users2', connection=conn)
many_users = users.batch_get(keys=[
     {'account_type': 'standard_user', 'last_name': 'Doe'},
     {'account_type': 'standard_user', 'last_name': 'Doering'},
     {'account_type': 'super_user', 'last_name': 'Liddel'},
 ])
for user in many_users:
     print user['first_name']




###########################
## Table Scan
###########################
from boto.dynamodb2.table import Table
users = Table('users2', connection=conn)
scan_results = users.scan(
    birthday__ne='null',
    limit=50
)

for result in scan_results:
    print result['username']

## list of comparison oeprators:
# EQ | NE | LE | LT | GE | GT | NOT_NULL | NULL | CONTAINS | NOT_CONTAINS | BEGINS_WITH | IN | BETWEEN








