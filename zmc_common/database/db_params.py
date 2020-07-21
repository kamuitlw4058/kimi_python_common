


def get_zmc_postgres_customer(custom_id):
    return {
        "host": "172.22.57.108",
        "port": 5432,
        "user": "postgres",
        "password": "postgres",
        "database": f"z_{str(custom_id).rjust(5,'0')}",
        'charset': 'utf8',
        'protocol':'postgres'
    }