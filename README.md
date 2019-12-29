# Kafka Playground - Twitter Streaming

# Requirement
- Python 3.6+
- Docker

# How to use:
- Create your Twitter access token & consumer key, and then fill it in `env.sample.sh`
- `virtualenv -p <which python> venv`
- `docker-compose up -d`
- `source venv/bin/activate`
- `pip install -r requirement.txt`
- `open producer.py > rename the value of "TRACK" based on the keyword that you want to use as filter`
- run `python producer.py`
- make sure that the topic has been created by checking on `localhost:9021` > Cluster 1 > click the topic name > check "Messages"
- `open consumer.py > rename the value of "TRACK" based on the keyword that you want to use as filter`
- run `python consumer.py`
- make sure that the consumer has been stored the data on elasticsearch by checking on `http://localhost:9200/_cat/indices?v&pretty` and the index appears on there
- try to search the document by using `http://localhost:9200/com.twitter.track.<YOUR TRACK NAME>/_search?q=lang:en`


## References:
- [Tutorial - Facebook SDK](https://pypi.org/project/python-facebook-api/)
- [Tutorial - Create Facebook Token](https://medium.com/@DrGabrielA81/python-how-making-facebook-api-calls-using-facebook-sdk-ea18bec973c8)
- [Documentation - Facebook SDK](https://facebook-sdk.readthedocs.io/en/latest/api.html)
- [Documentation - Twitter SDK](https://tweepy.readthedocs.io/en/latest/getting_started.html)
- [Documentation - Twitter streaming message types](https://developer.twitter.com/en/docs/tweets/filter-realtime/guides/streaming-message-types)