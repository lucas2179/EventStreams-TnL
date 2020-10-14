eventstream = "YOUR JSON CREDENTIALS  FOR EVENT STREAMS"

es_brokers = ','.join(eventstream['kafka_brokers_sasl'])
es_apikey = eventstream['api_key']
es_rest = eventstream['kafka_admin_url']

access_token = 'TWITTER ACCESS TOKEN'
access_token_secret = 'TWITTER ACCESS TOKEN SECRET'
consumer_key = 'TWITTER CONSUMER KEY'
consumer_secret = 'CONSUMER SECRET TWITTER'