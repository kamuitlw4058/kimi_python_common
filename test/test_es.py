from kimi_common.search.es import ElasticsearchClient

esc = ElasticsearchClient(['10.15.1.177:9200'])
esc.set('1232','23123sf3333')
print(esc.get('1232333'))