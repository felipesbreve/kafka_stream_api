from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
import logging

# Função para criar a conexão Spark
def create_spark_connection():
    try:
        s_conn = SparkSession.builder \
            .appName('SparkDataStreaming') \
            .config(
                'spark.jars.packages',
                'com.datastax.spark:spark-cassandra-connector_2.13:3.4.1,'
                'org.apache.spark:spark-sql-kafka-0-10_2.13:3.4.0'
            ) \
            .config('spark.cassandra.connection.host', 'localhost') \
            .getOrCreate()
        s_conn.sparkContext.setLogLevel('ERROR')
        logging.info('Conexão Spark criada com sucesso!')
        return s_conn
    except Exception as e:
        logging.error(f'Não foi possível criar conexão com SPARK: {e}')
        return None

# Função para criar a conexão com o Kafka e retornar o DataFrame
def connect_to_kafka(spark_conn):
    try:
        spark_df = spark_conn.readStream \
            .format('kafka') \
            .option('kafka.bootstrap.servers', 'broker:29092') \
            .option('subscribe', 'users_created') \
            .option('startingOffsets', 'earliest') \
            .load()

        logging.info('DataFrame criado com sucesso!')
        return spark_df
    except Exception as e:
        logging.error(f'Não foi possível criar conexão com o KAFKA: {e}')
        return None

# Função para criar a conexão com o Cassandra
def create_cassandra_connection():
    try:
        cluster = Cluster(['localhost'])
        session = cluster.connect()
        logging.info('Conexão com Cassandra criada com sucesso!')
        return session
    except Exception as e:
        logging.warning(f'Não foi possível criar conexão com CASSANDRA: {e}')
        return None

# Função para criar o Keyspace no Cassandra
def create_keyspace(session):
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {
                'class': 'SimpleStrategy',
                'replication_factor': '1'
            }
        """)
        logging.info('Keyspace criado com sucesso!')
    except Exception as e:
        logging.error(f'Não foi possível criar o Keyspace: {e}')

# Função para criar a tabela no Cassandra
def create_table(session):
    try:
        session.execute("""
            CREATE TABLE IF NOT EXISTS spark_streams.created_users(
                id UUID PRIMARY KEY,
                first_name TEXT,
                last_name TEXT,
                gender TEXT,
                address TEXT,
                postcode TEXT,
                email TEXT,
                username TEXT,
                dob TEXT,
                registered_date TEXT,
                phone TEXT,
                picture TEXT    
            )
        """)
        logging.info('Tabela criada com sucesso!')
    except Exception as e:
        logging.error(f'Não foi possível criar a Tabela: {e}')

# Função para processar os dados do Kafka e inserir no Cassandra
def process_data(**kwargs):
    spark_conn = kwargs['ti'].xcom_pull(task_ids='create_spark_connection')
    if spark_conn:
        spark_df = connect_to_kafka(spark_conn)
        schema = StructType([
            StructField('id', StringType(), False),
            StructField('first_name', StringType(), False),
            StructField('last_name', StringType(), False),
            StructField('gender', StringType(), False),
            StructField('address', StringType(), False),
            StructField('postcode', StringType(), False),
            StructField('email', StringType(), False),
            StructField('username', StringType(), False),
            StructField('dob', StringType(), False),
            StructField('registered_date', StringType(), False),
            StructField('phone', StringType(), False),
            StructField('picture', StringType(), False)
        ])
        selection_df = spark_df.selectExpr("CAST(value AS STRING)") \
            .select(from_json(col('value'), schema).alias('data')).select('data.*')

        session = create_cassandra_connection()
        if session:
            create_keyspace(session)
            create_table(session)

            streaming_query = selection_df.writeStream \
                .format('org.apache.spark.sql.cassandra') \
                .option('checkpointLocation', '/tmp/checkpoint') \
                .option('keyspace', 'spark_streams') \
                .option('table', 'created_users') \
                .start()
            streaming_query.awaitTermination()
        else:
            logging.error('Não foi possível criar a conexão com Cassandra.')
    else:
        logging.error('Não foi possível criar a conexão com Spark.')

# Definindo o DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

with DAG(
    dag_id='spark_to_cassandra_dag',
    default_args=default_args,
    schedule_interval=None,  # Execute o DAG manualmente ou defina um intervalo
    catchup=False,
) as dag:

    create_spark_connection_task = PythonOperator(
        task_id='create_spark_connection',
        python_callable=create_spark_connection,
    )

    process_data_task = PythonOperator(
        task_id='process_data',
        python_callable=process_data,
        provide_context=True,
    )

    create_spark_connection_task >> process_data_task
