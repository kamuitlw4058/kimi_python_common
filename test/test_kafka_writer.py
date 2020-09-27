import time
import random
import json

from kimi_common.mq.kafka import KafkaClient


client = KafkaClient(['47.103.86.120:9092'],'test')
i = 0
print('enter loop')
while True:
    count = random.randint(10000,10000)
    for r in range(count):
        d = {
            'user_id':f"{r}",
        }
        #d = {"note_type": 2.0, "favorite_number": 0.0, "real_source": 21.0, "comment_number_x": 0.0, "share_number": 0.0, "praise_number": 0.0, "play_number": 0.0, "video_duration": 0.0, "note_duration": 0, "gender": NaN, "video_public_release_days": 71, "label_level1_id": 124.0, "label": 0, "note_ctr": 0.0, "note_id_video_play_percent_mean": 0.0, "user_active_date_7d": 4.0, "user_active_date_14d": 9.0, "user_clk_label_topn": "32", "age": -1, "label_id": 124.0, "note_id": 6420986, "user_id": 0},

        #print(json.dumps(d))
        client.write(json.dumps(d))
        i+=1

    time.sleep(1)

