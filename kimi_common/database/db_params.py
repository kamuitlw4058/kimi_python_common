


def __zmc_postgres_db():
    return {
        "host": "172.22.57.108",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        'charset': 'utf8',
        'protocol':'postgres'
    }



def zmc_postgres_customer_params(custom_id):
    params = __zmc_postgres_db()
    params['database'] =f"z_{str(custom_id).rjust(5,'0')}"
    return params