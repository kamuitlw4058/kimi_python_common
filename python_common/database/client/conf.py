
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



clickhouse_cmd_local_adx ={
    'protocol':'clickhouse',
    'user':'default',
    #'user':'default',
    'password':'',
    #'password':'',
    'host':'localhost',
    'port':9000,
    'database':'xn_adx'
}

clickhouse_xn_adx ={
    'protocol':'clickhouse',
    'user':'chaixiaohui',
    #'user':'default',
    'password':'AAAaaa111!',
    #'password':'',
    'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
    'port':8123,
    'database':'xn_adx'
}


clickhouse_cmd_xn_adx ={
    'protocol':'clickhouse',
    'user':'chaixiaohui',
    #'user':'default',
    'password':'AAAaaa111!',
    #'password':'',
    'host':'cc-uf6tj4rjbu5ez10lb.ads.aliyuncs.com',
    'port':3306,
    'database':'xn_adx'
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


clickhouse_cmd_xn_adp_knowledge ={
    'protocol':'clickhouse',
    'user':'chaixiaohui',
    #'user':'default',
    'password':'AAAaaa111!',
    #'password':'',
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




db_params = {
    'default':clickhouse_xn_adp,
    'ch_xn_adp':clickhouse_xn_adp,
    'ch_xn_adp_knowledge':clickhouse_xn_adp_knowledge,
    'ch_cmd_xn_adp_knowledge':clickhouse_cmd_xn_adp_knowledge,
    'mysql_adsense_pizarr':mysql_adsense_pizarr,
    'mysql_adsense_pizarr_test':mysql_adsense_pizarr_test,
    'ch_cmd_xn_adx':clickhouse_cmd_xn_adx,
    'ch_xn_adx':clickhouse_xn_adx,
    'ch_cmd_local_adx':clickhouse_cmd_local_adx,
}