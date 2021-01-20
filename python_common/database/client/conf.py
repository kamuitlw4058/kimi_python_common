
mysql_adsense_pizarr ={
    'protocol':'mysql',
    'user':'ads_read',
    'password':'3UmA#kkSD0th8VM',
    'host':'rr-uf6z09z4vxcr8li31.mysql.rds.aliyuncs.com',
    'port':3306,
    'database':'adsense_pizarr'
}

mysql_adsense_pizarr_test ={
    'protocol':'mysql',
    'user':'ad_test',
    'password':'S4BGRU5yQEm6yn9KHOJb',
    'host':'rm-uf6muqdsogu7h721o.mysql.rds.aliyuncs.com',
    'port':3306,
    'database':'adsense_pizarr_db_test'
}

mysql_adx_strategy_test ={
    'protocol':'mysql',
    'user':'root',
    'password':'123456',
    'host':'172.16.11.74',
    'port':3306,
    'database':'adx_strategy'
}


mysql_adp_ml_test ={
    'protocol':'mysql',
    'user':'root',
    'password':'123456',
    'host':'172.16.11.74',
    'port':3306,
    'database':'adp_ml',
    'charset':'utf8'
}


mysql_adx_strategy ={
    'protocol':'mysql',
    'user':'root',
    'password':'123456',
    'host':'192.168.3.64',
    'port':3307,
    'database':'adx_strategy'
}




clickhouse_cmd_local_adx ={
    'protocol':'clickhouse',
    'user':'default',
    'password':'',
    'host':'localhost',
    'port':9000,
    'database':'xn_adx'
}

clickhouse_xn_adx ={
    'protocol':'clickhouse',
    'user':'chaixiaohui',
    'password':'AAAaaa111!',
    'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
    'port':8123,
    'database':'xn_adx'
}



clickhouse_cmd_xn_adx ={
    'protocol':'clickhouse',
    'user':'chaixiaohui',
    'password':'AAAaaa111!',
    'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
    'port':3306,
    'database':'xn_adx'
}

clickhouse_cmd_xn_adx_cluster ={
    'protocol':'clickhouse',
    'user':'xnad_suanfa',
    'password':'M6119Hb#Aj80TtosdkjUDN89',
    'host':'cc-uf6y3p4u3ff10s973.ads.aliyuncs.com',
    'port':3306,
    'database':'ad_adx'
}


clickhouse_xn_adp ={
    'protocol':'clickhouse',
    'user':'chaixiaohui',
    'password':'AAAaaa111!',
    'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
    'port':8123,
    'database':'xn_adp'
}


clickhouse_cmd_xn_adp_knowledge ={
    'protocol':'clickhouse',
    'user':'chaixiaohui',
    'password':'AAAaaa111!',
    'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
    'port':3306,
    'database':'xn_adp_knowledge'
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

hive_xn_bigdata = {
    'protocol':'hive',
    #'user':'suanfa',
    #'password':'suanfa123',
    'host':'192.168.66.115',
    'port':1000,
    'database':'dc_ods_voice'
}


db_params = {
    'default':clickhouse_xn_adp,
    'ch_xn_adp':clickhouse_xn_adp,
    'ch_xn_adp_knowledge':clickhouse_xn_adp_knowledge,
    'ch_cmd_xn_adp_knowledge':clickhouse_cmd_xn_adp_knowledge,
    'mysql_adsense_pizarr':mysql_adsense_pizarr,
    'mysql_adsense_pizarr_test':mysql_adsense_pizarr_test,
    'ch_cmd_xn_adx':clickhouse_cmd_xn_adx,
    'ch_cmd_xn_adx_cluster':clickhouse_cmd_xn_adx_cluster,
    'ch_xn_adx':clickhouse_xn_adx,
    'ch_cmd_local_adx':clickhouse_cmd_local_adx,
    'mysql_adx_strategy_test':mysql_adx_strategy_test,
    'mysql_adx_strategy':mysql_adx_strategy,
    'mysql_adp_ml_test':mysql_adp_ml_test,
    'hive_xn_bigdata':hive_xn_bigdata
}