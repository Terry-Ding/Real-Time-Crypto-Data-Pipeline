import logging 
from datetime import datetime 
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col 

def create_keyspace(session):
    """ this function creates keyspace """

def create_table(session):
    """ this function creates table """

def insert_data(session, **kwargs):
    """ this function inserts data from kafka """

def create_spark_connection():
    """ this function creates spark connection """

def create_cassandra_connection():
    """ this function creates cassandra connection """