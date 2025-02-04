import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from config import pg_host, pg_port, pg_user, pg_passwd

conn_pools = {}
conn_pools98 = {}

def getconn(database):
    if database not in conn_pools:
        dsn = "host='%s' port='%s' dbname='%s' user='%s' password='%s'" % (
            pg_host, pg_port, database, pg_user, pg_passwd)
        conn_pools[database] = ThreadedConnectionPool(1, 5, dsn=dsn)
    return conn_pools[database].getconn()

def db201exec(database, sqlstring):
    conn = getconn(database)
    cur = conn.cursor()
    try:
        cur.execute(sqlstring)
        conn.commit()
        cur.close()
    except (Exception, psycopg2.Error) as error:
        conn.rollback()
        cur.close()
        return error
    finally:
        cur.close()
        conn_pools[database].putconn(conn)