import csv
import logging
import sys

import psycopg2 as pg
import yaml
from tqdm import tqdm

from bd_creator.tools.iterator_file import IteratorFile
from bd_creator.tools.init_preprocessor import InitialPreprocessor

csv.field_size_limit(sys.maxsize)

logging.basicConfig(level=logging.DEBUG)


class DataBaseConnection:
    def __init__(self, db_config_file):
        with open(db_config_file) as config_file:
            config = yaml.load(config_file, Loader=yaml.FullLoader)
        self.db_config = config.get("pg")

    def __enter__(self):
        logging.info("\nCreating DB connection...")
        self.connection = pg.connect(
            host=self.db_config.get("host"),
            port=int(self.db_config.get("port")),
            dbname=self.db_config.get("dbname"),
            user=self.db_config.get("user")
        )
        logging.info("Connection created!")
        return self.connection

    def __exit__(self, type, value, traceback):
        logging.info("Closing the DB connection!")
        self.connection.close()


class DBTools:
    def __init__(self, db_config_path, schema_config, max_text_len, batch_size, file_name):
        self.batch_size = batch_size
        self.table_source = file_name
        self.initial_text_preprocessor = InitialPreprocessor(max_text_len=max_text_len)
        self.db_config_path = db_config_path
        with open(schema_config) as schema_file:
            self.schema = yaml.load(schema_file, Loader=yaml.FullLoader)

    def create_tables(self):
        with DataBaseConnection(self.db_config_path) as connection:
            cur = connection.cursor()
            for table in self.schema:
                try:
                    name = table.get("name")
                    schema = table.get("schema")
                    ddl = f"""CREATE TABLE IF NOT EXISTS {name} ({schema})"""
                    cur.execute(ddl)
                except Exception as e:
                    logging.error(f"Could not create table {name}:", e)
                    raise
            logging.info("Tables successfully created in the DB!")
            connection.commit()

    def load_tables_bulk(self):
        with DataBaseConnection(self.db_config_path) as connection:
            cur = connection.cursor()
            batch = []
            with open(self.table_source, 'r') as input:
                normalized_input = (line.replace('\0', '').replace('\n', ' ') for line in input)
                reader = csv.reader(normalized_input,  delimiter=",")
                next(reader, None)
                for i, row in tqdm(enumerate(reader), desc='load data to bd'):
                    if self.initial_text_preprocessor.validate_row(row):
                        row = self.initial_text_preprocessor.normalize_row(row)
                        if len(batch) < self.batch_size:
                            batch.append(row)
                        elif len(batch) == self.batch_size:
                            f = IteratorFile(
                                ('\t'.join([row[1], row[2], row[3], row[4]])for row in batch))
                            cur.copy_from(
                                f, 'smth', columns=('smth', 'smth', 'smth', 'smth'))
                            batch = []
            connection.commit()
        logging.info("Data were successfully loaded in the DB :) \n")

    def create_index(self):
        with DataBaseConnection(self.db_config_path) as connection:
            cur = connection.cursor()
            cur.execute("""CREATE INDEX ON smth (smth, smth, smth);""")
            logging.info(f"Index created!")
            connection.commit()

    def get_info(self):
        with DataBaseConnection(self.db_config_path) as connection:
            cur = connection.cursor()
            cur.execute("SELECT pg_table_size(quote_ident(%s)), pg_indexes_size(quote_ident(%s))",
                        ('smth', 'smth'))
            row = cur.fetchone()
            logging.info(f"Table size mb: {row[0] / 1000000}; Index size mb: {row[1] / 1000000};")
            connection.commit()

    def vacuum(self):
        with DataBaseConnection(self.db_config_path) as connection:
            old_isolation_level = connection.isolation_level
            connection.set_isolation_level(0)
            cur = connection.cursor()
            query = "VACUUM (VERBOSE, FULL) smth"
            cur.execute(query)
            logging.info(f"VACUUM done!")
            connection.set_isolation_level(old_isolation_level)

            for n in connection.notices:
                print(n)
