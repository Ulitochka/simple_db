import os
import time
import logging
import argparse

from bd_creator.db_tools import DBTools
from data_generator import DataGenerator


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--file_path', type=str, required=True)
    parser.add_argument('--output_path', type=str, required=True)
    parser.add_argument('--bd_config', type=str, required=True)
    parser.add_argument('--bd_schema', type=str, required=True)
    parser.add_argument('--max_text_len', type=int, required=True)
    parser.add_argument('--batch_size', type=int, required=True)
    parser.set_defaults(feature=True)
    args = parser.parse_args()

    start_time = time.time()
    db_tools = DBTools(
        db_config_path=args.bd_config,
        schema_config=args.bd_schema,
        max_text_len=args.max_text_len,
        batch_size=args.batch_size,
        file_name=args.file_path)

    db_tools.create_tables()
    db_tools.load_tables_bulk()
    db_tools.create_index()
    db_tools.get_info()
    logging.info(f"Total time: {str(time.time() - start_time)}")

    input_file_name = os.path.basename(args.file_path).split('.')[0]

    data_generator = DataGenerator(input_file_name=input_file_name)
    data_generator.get_values(output_path=args.output_path)
    data_generator.c.close()
