import distutils.dir_util
import logging
import os
import time
import uuid

import psycopg2
from tqdm import tqdm


def init_logging(input_file_name):
    fmt = logging.Formatter('%(asctime)-15s %(message)s')

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    log_dir_name = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'some_folder/logs')
    log_file_name = time.strftime('%Y_%m_%d-%H_%M_%S-') + str(uuid.uuid4())[:8] + '_%s.txt' % (input_file_name,)
    logging.info('Logging to {}'.format(log_file_name))
    logfile = logging.FileHandler(os.path.join(log_dir_name, log_file_name), 'w')
    logfile.setFormatter(fmt)
    logger.addHandler(logfile)


class DataGenerator:
    TYPES = [1, 2, 3]
    MAX_ID = 220
    NON_VALID_IDS = {100, 200, 300, 150, 250}

    """
    Class to read from the database.
    """

    def __init__(self, input_file_name):

        connection = psycopg2.connect(
            host='localhost',
            port=54320,
            dbname='main_db',
            user='postgres',
        )

        self.c = connection.cursor()
        self.input_file_name = input_file_name
        init_logging(input_file_name)

    def get_all_values(self):
        self.c.execute("""SELECT t.smth, t.smth, t.smth, t.smth, t.smth, t.smth, t.smth FROM some_table t;""")
        for result in tqdm(self.c.fetchall()):
            yield (
                result[0].date().year,
                result[1],
                result[2],
                result[3],
                result[4],
                result[5],
            )
        self.c.close()

    def get_values(self, output_path):
        base_path = os.path.join(output_path, self.input_file_name)
        for proj_id in [i for i in range(1, self.MAX_ID) if i not in self.NON_VALID_IDS]:
            logging.info(f"------ Project id={proj_id} -------")
            for s_type in self.TYPES:
                self.c.execute(
                    f"""SELECT text FROM queries WHERE searchType = {s_type} AND proj = {proj_id};""")
                result = self.c.fetchall()
                if result:
                    logging.info(
                        f"Query: search_type={str(s_type)}; proj={str(proj_id)} has {str(len(result))} results!")
                    new_path = os.path.join(base_path, str(proj_id))
                    distutils.dir_util.mkpath(new_path)
                    text_file = os.path.join(new_path, '%s.txt' % (str(s_type)))
                    with open(text_file, 'w') as outf:
                        for el in result:
                            outf.write(el[0] + '\n')
                else:
                    logging.info(f"Query: search_type={str(s_type)}; proj={str(proj_id)} has no results!")
