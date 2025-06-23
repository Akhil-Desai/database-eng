import psycopg2
import mysql


def get_connection(db_config):
        """
        Create and return a new database connection for the configured backend.
        """
        if db_config['type']== 'postgresql':
            db_config_no_type = {k:v for k,v in db_config.items() if k != "type"}
            return psycopg2.connect(**db_config_no_type)
        elif db_config['type'] == 'mysql':
            db_config_no_type = {k:v for k,v in db_config.items() if k != "type"}
            return mysql.connector.connect(**db_config)
