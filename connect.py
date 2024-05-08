from configparser import ConfigParser
import psycopg2
from sqlalchemy import create_engine

def config(filename='./config/database.ini', section='postgresql'):
    # create a parser
    parser = ConfigParser()
    # read config file
    parser.read(filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def connect():
    """ Connect to the PostgreSQL database server """
    conn = None
    try:
        # read connection parameters
        params = config()

        # connect to the PostgreSQL server
        print('Connecting to the PostgreSQL database...')
        conn = psycopg2.connect(**params)
		
        return conn 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)

def create_engine_conn():
    engine = None
    try: 
        params = config()
        host = str(params['host'])
        port = str(params['port'])
        user = params['user']
        password = params['password']
        database = params['database']
        engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
        return engine
    except(Exception, psycopg2.DatabaseError) as error:
        print(error)

if __name__ == '__main__':
    connect()
    create_engine_conn()