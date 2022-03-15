import time
import logging

from tqdm import tqdm
import psycopg2


logging.basicConfig(level=logging.DEBUG)


class DataGenerator:
    """
    Class to read from the database.
    """

    def __init__(self):

        connection = psycopg2.connect(
            host='localhost',
            port=54320,
            dbname='test_db',
            user='postgres',
        )

        self.c = connection.cursor()

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

    def get_values(self, types):
        start_time = time.time()
        self.c.execute(f"""SELECT smth FROM smth WHERE smth = {types};""")
        for result in tqdm(self.c.fetchall()):
            yield result[0]
        logging.info(f"Total time: {str(time.time() - start_time)}")


if __name__ == '__main__':
    data_generator = DataGenerator()

    for types in [1, 2, 3]:
        for el in data_generator.get_values(types):
            pass
    data_generator.c.close()
