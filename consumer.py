
from kafka import KafkaConsumer
import json
import watson
import config as cfg
import requests
from bs4 import BeautifulSoup
topic_name = 'nowPlaying'


consumer = KafkaConsumer(
    topic_name,
    bootstrap_servers=cfg.es_brokers,
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='token',
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms =  5000,
    fetch_max_bytes = 128,
    max_poll_records = 100,
    sasl_plain_password=cfg.es_apikey,
    value_deserializer=lambda x: json.loads(x.decode('utf-8')))
lista = []
for message in consumer:
    tweets = json.loads(json.dumps(message.value))
    # print(json.dumps(tweets, indent=2))

    for j in tweets['entities']['urls']:
        if "open.spotify.com/track" in j['expanded_url']:
            # watson.analyze(tweets['text'])
            page = requests.get(j['expanded_url'])
            soup = BeautifulSoup(page.content, 'html.parser')
            # print(soup.title.string)
            music_info = soup.title.string.split(", a song by")
            music = music_info[0]
            artist = music_info[1][:-11]
    
            lista.append({"music": music, "artist": artist})
            # print(lista)
            my_dict = {k['music']:lista.count(k) for k in lista}
            print("musica = " + music + " artist = " + artist )
            print(my_dict)
        # print(j)