import time
from kimi_common.kv.redis import RedisClient
import json

r = RedisClient('10.15.0.106',63790)
d = {"note_type": 2.0, "favorite_number": 0.0, "real_source": 21.0, "comment_number_x": 0.0, "share_number": 0.0, 
"praise_number": 0.0, "play_number": 0.0, "video_duration": 0.0,
 "note_duration": 0, "gender": '123', "video_public_release_days": 71,
  "label_level1_id": 124.0, "label": 0, "note_ctr": 0.0, 
  "note_id_video_play_percent_mean": 0.0, "user_active_date_7d": 4.0,
   "user_active_date_14d": 9.0, "user_clk_label_topn": "32", "age": -1, 
   "label_id": 124.0, "note_id": 6420986, "user_id": 0},
#d = {"note_type": 2.0, "favorite_number": 0.0, "real_source": 21.0,'user_id':123}
#                'note_type',
# d ={
# "note_type": 2.0,
#  "real_source": 21.0,
#  "label_level1_id": 124.0,
#  "praise_number": 0.0,
#  "note_id": 6420986
# }
k_list = []
v_list =[]

for i in range(100000):
    k_list.append(f'user:{i}')
    v_list.append(json.dumps(d))

r.batch_set(k_list,v_list)

start = time.time()
print(r.get('user:0'))
print(r.batch_get(['user:0']))
r =  r.batch_get(['user:0'])
for k,v in r:
    print(k)
    print(v)

end = time.time()
print(f'elapsed :{end -start}')#