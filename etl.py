# IMPORTS
import configparser
import psycopg2
from sql_queries import drop_table_queries \
                      , create_table_queries \
                      , copy_table_queries \
                      , insert_table_queries \
                      , staging_checks \
                      , insert_checks

config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))
HOST = config.get("CLUSTER", "HOST")
DB_NAME = config.get("CLUSTER", "DB_NAME")
DB_USER = config.get("CLUSTER", "DB_USER")
DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
DB_PORT = config.get("CLUSTER", "DB_PORT")
ARN_IAM_ROLE = config.get("IAM_ROLE", "arn")


def drop_tables(cur, conn):
    """
    Executes the queries defined in 'drop_table_queries'.
    
    Args:
        cur (conn.cursor()): Cursor to execute database commands.
        conn (psycopg2.connect): Database connection details.
    Returns:
        None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Executes the queries defined in 'create_table_queries'.
    
    Args:
        cur (conn.cursor()): Cursor to execute database commands.
        conn (psycopg2.connect): Database connection details.
    Returns:
        None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def load_staging_tables(cur, conn):
    """
    Executes the queries defined in 'copy_table_queries'.
    
    Args:
        cur (conn.cursor()): Cursor to execute database commands.
        conn (psycopg2.connect): Database connection details.
    Returns:
        None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Executes the queries defined in 'insert_table_queries'.
    
    Args:
        cur (conn.cursor()): Cursor to execute database commands.
        conn (psycopg2.connect): Database connection details.
    Returns:
        None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

        
def quality_checks(cur, conn, checks):
    """
    Executes data quality queries defined in 'checks'.
    
    Args:
        cur (conn.cursor()): Cursor to execute database commands.
        conn (psycopg2.connect): Database connection details.
        checks: dictionary of data quality checks and expected results, example:
            {'check_sql': 'SELECT COUNT(*) FROM i94visal', 'expected_result': 3}

    Returns:
        None
    """
    passed_count = 0
    failed_count = 0
    passed_tests = []
    failed_tests = []

    for check in checks:
        chk_sql = check.get('check_sql')
        exp_result = check.get('expected_result')

        cur.execute(chk_sql)
        result = cur.fetchall()
    
        if exp_result != result[0][0]:
            failed_count += 1
            failed_tests.append((chk_sql, exp_result))
        else:
            passed_count += 1
            passed_tests.append((chk_sql, exp_result))
        
    if passed_count > 0:
        print("PASSED QUALITY CHECKS:")
        for passed_test in passed_tests:
            print(passed_test)
        
    if failed_count > 0:
        print("FAILED QUALITY CHECKS:")
        for failed_test in failed_tests:
            print(failed_test)
        raise Exception('Data Check FAILED!')


def main():
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"\
                            .format(HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    cur = conn.cursor()

    print('Begin ETL:')

    drop_tables(cur, conn)
    print('\n' + 'drop_tables...COMPLETE')

    create_tables(cur, conn)
    print('\n' + 'create_tables...COMPLETE')

    load_staging_tables(cur, conn) #Approximate Staging Time: 2min for ~50 Million Records with 2 Nodes
    print('\n' + 'load_staging_tables...COMPLETE')

    print('')
    quality_checks(cur, conn, staging_checks)
    print('\n' + 'staging_quality_checks...COMPLETE')

    insert_tables(cur, conn) #Approximate Insert Time: 1min for ~50 Million Records with 2 Nodes
    print('\n' + 'insert_tables...COMPLETE')

    print('')
    quality_checks(cur, conn, insert_checks)
    print('\n' + 'insert_quality_checks...COMPLETE')

    conn.close()
    print('\n' + 'End of ETL' + '\n')


if __name__ == "__main__":
    main()
#EOF