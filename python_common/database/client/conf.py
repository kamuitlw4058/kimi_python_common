
mysql_adsense_pizarr ={
    'protocol':'mysql',
    'user':'ads_read',
    'password':'3UmA#kkSD0th8VM',
    'host':'rr-uf6z09z4vxcr8li31.mysql.rds.aliyuncs.com',
    'port':3306,
    'database':'adsense_pizarr'
}


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
    'mysql_adsense_pizarr':mysql_adsense_pizarr,
}