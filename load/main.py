# -*- coding: utf-8 -*-
import argparse
import os
import json
from pyspark.sql import SparkSession, DataFrame


class PysparkJob(object):
    def __init__(self, job_name: str = None, jars: str = None, **kwargs):
        """
        A constructor to initialize a SparkContext and kick off a standalone spark job

        :param job_name:
        :param jars:
        :param kwargs:
        """

        if job_name is None:
            job_name = PysparkJob.__name__

        if jars is not None:
            valid_jars = [jar for jar in jars.split(',') if os.path.isfile(jar)]
            jars = ','.join(valid_jars)

        self.input_data: DataFrame = None

        self.spark = SparkSession.builder \
            .appName(job_name) \
            .config("spark.jars", jars) \
            .getOrCreate()

        sc = self.spark.sparkContext
        sc.setLogLevel("INFO")
        self.logger = sc._jvm.org.apache.log4j.LogManager.getLogger(PysparkJob.__name__)

    def load_csv(self, input_path: str, header: bool = True, infer_schema: bool = True, **kwargs):

        """
        This method will be generalized to load any CSV file and assign to `input_data`

        :param input_path: the path of CSV
        :param header: True if include the header of CSV
        :param infer_schema: True if need to infer the data schema automatically
        :param kwargs: other params
        :return:
        """

        is_header = "true" if header else "false"
        is_infer_schema = "true" if infer_schema else "false"

        self.input_data = self.spark.read.option("header", is_header).option("inferSchema", is_infer_schema).csv(input_path)
        return self

    def insert_into_mysql(self, host: str, db: str, table: str, user: str, password: str):
        """
        This method will be generalized to append the data from `input_data` to mysql `db`.`table`
        :param db: mysql host
        :param db: mysql database
        :param table: mysql table
        :param user: mysql username
        :param password: mysql password
        :return:
        """

        if self.input_data is not None:
            self.logger.info(f"--- Insert data to Mysql: `{db}`.`{table}` ---")
            self.input_data.write.mode("append") \
                .format("jdbc") \
                .option("driver", "com.mysql.cj.jdbc.Driver") \
                .option("url", f"jdbc:mysql://{host}:3306/{db}") \
                .option("dbtable", table) \
                .option("user", user) \
                .option("password", password) \
                .save()
        else:
            self.logger.warn("--- input_data is empty! load_csv might not be run properly ---")


def main():
    """
    Main driver to run the PysparkJob - use to load the CSV data (specify in configurable file `data_config.json` to Mysql

    :return:
    """

    parser = argparse.ArgumentParser()
    parser.add_argument("--conf",
                        type=str,
                        help="JSON configuration file",
                        default='./data_config.json')
    parser.add_argument("--mysql-java-connector",
                        type=str,
                        help="Mysql java connector",
                        default="./jars/mysql-connector-java-8.0.28.jar")
    parser.add_argument("--mysql-db",
                        type=str,
                        help="Mysql database",
                        default=os.environ.get('MYSQL_DATABASE'))
    parser.add_argument("--mysql-host",
                        type=str,
                        help="Mysql host",
                        default="localhost")
    parser.add_argument("--mysql-user",
                        type=str,
                        help="Mysql username",
                        default="root")
    parser.add_argument("--mysql-password",
                        type=str,
                        help="Mysql password",
                        default=os.environ.get('MYSQL_ROOT_PASSWORD'))

    arguments, _ = parser.parse_known_args()
    print(arguments)
    if os.path.isfile(arguments.conf):
        with open(arguments.conf) as r:
            tables = json.load(r)

        pyspark_job = PysparkJob(job_name="Load CSV to Mysql", jars=arguments.mysql_java_connector)
        for table in tables:
            pyspark_job.load_csv(**table).insert_into_mysql(
                db=arguments.mysql_db,
                table=table['table'],
                host=arguments.mysql_host,
                user=arguments.mysql_user,
                password=arguments.mysql_password
            )


if __name__ == '__main__':
    main()

