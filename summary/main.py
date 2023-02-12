# -*- coding: utf-8 -*-
import argparse
import os
import json
import pyspark.sql.functions as F
from pyspark.sql import SparkSession, DataFrame


class SummaryJob(object):
    def __init__(self, job_name: str = None, jars: str = None, **kwargs):
        """
        A constructor to initialize a SparkContext and kick off a standalone spark job

        :param job_name:
        :param jars:
        :param kwargs:
        """

        if job_name is None:
            job_name = SummaryJob.__name__

        if jars is not None:
            valid_jars = [jar for jar in jars.split(',') if os.path.isfile(jar)]
            jars = ','.join(valid_jars)

        self.people_data: DataFrame = None
        self.places_data: DataFrame = None

        self.spark = SparkSession.builder \
            .appName(job_name) \
            .config("spark.jars", jars) \
            .getOrCreate()

        sc = self.spark.sparkContext
        sc.setLogLevel("INFO")
        self.logger = sc._jvm.org.apache.log4j.LogManager.getLogger(SummaryJob.__name__)

    def read_data(self, host: str, db: str, user: str, password: str, **kwargs):

        """
        This method will be generalized to read data from msyql table

        :param host: mysql host
        :param db: mysql database
        :param table: mysql table
        :param user: mysql username
        :param password: mysql password
        :param kwargs:
        :return:
        """

        def read(table: str):
            return self.spark.read.format("jdbc") \
                .option("driver","com.mysql.cj.jdbc.Driver") \
                .option("url", f"jdbc:mysql://{host}:3306/{db}") \
                .option("dbtable", table) \
                .option("user", user) \
                .option("password", password) \
                .load()

        # Hard-code for these two specific tables
        self.people_data = read("people")
        self.places_data = read("places")
        return self

    def summarize(self, output_path: str):
        """
        Aggregate data to a list of the countries, and a count of how many people were born in that country

        :param output_path:
        :return:
        """

        if self.people_data is not None and self.places_data is not None:
            # assume `place_of_birth` is at city level in places table
            # will perform a left join from places to people using normalized city (trim & lowercase)

            people = self.people_data.withColumn("join_key", F.lower(F.trim(F.col("place_of_birth"))))
            places = self.places_data.withColumn("join_key", F.lower(F.trim(F.col("city"))))
            people_countries = places.join(people, "join_key", "left") \
                .select(places["country"], people["id"].alias("people_id")) \
                .groupBy("country") \
                .agg(F.countDistinct(F.col("people_id")).alias("nb_people")) \
                .collect()

            people_countries = {row.country: row.nb_people for row in people_countries}
            with open(output_path, 'w') as w:
                json.dump(people_countries, w)

        else:
            self.logger.warn("--- people_data or places_data is empty! read_data(...) might not be run properly ---")


def main():
    """
    Main driver to run the SummaryJob - load people & places data in Mysql and calculate the summary

    :return:
    """

    parser = argparse.ArgumentParser()
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
    parser.add_argument("--output-path",
                        type=str,
                        help="A path to write the summary",
                        default='./data/summary_output.json')

    arguments, _ = parser.parse_known_args()
    print(arguments)

    summary_job = SummaryJob(job_name="Summary data in Mysql", jars=arguments.mysql_java_connector)
    summary_job.read_data(
        host=arguments.mysql_host,
        db=arguments.mysql_db,
        user=arguments.mysql_user,
        password=arguments.mysql_password
    ).summarize(output_path=arguments.output_path)


if __name__ == '__main__':
    main()

