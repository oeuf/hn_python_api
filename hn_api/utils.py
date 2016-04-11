import psycopg2


def get_connection(host='localhost', dbname='hn', user='ng', password=''):
    conn_string = 'host={} dbname={} user={} password={}'.format(host, dbname, user, password)
    return psycopg2.connect(conn_string)
