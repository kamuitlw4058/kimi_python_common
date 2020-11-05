
clickhouse_xn_adp ={
    'protocol':'clickhouse',
    'user':'chaixiaohui',
    #'user':'default',
    'password':'AAAaaa111!',
    #'password':'',
    'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
    'port':8123,
    'database':'xn_adp'
}


clickhouse_xn_adp_knowledge ={
    'protocol':'clickhouse',
    'user':'chaixiaohui',
    #'user':'default',
    'password':'AAAaaa111!',
    #'password':'',
    'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
    'port':8123,
    'database':'xn_adp_knowledge'
}


db_params = {
    'default':clickhouse_xn_adp,
    'ch_xn_adp':clickhouse_xn_adp,
    'ch_xn_adp_knowledge':clickhouse_xn_adp_knowledge,
}