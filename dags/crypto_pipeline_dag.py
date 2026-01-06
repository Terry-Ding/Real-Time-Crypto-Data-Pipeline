from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.sensors.external_task import ExternalTaskSensor

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 27),
}

with DAG(
    dag_id="crypto_pipeline_orchestrator",
    default_args=default_args,
    schedule=None, # Triggered manually
    catchup=False
) as dag:

    # 1. Trigger Data Ingestion DAG
    # This fires the ingestion dag which runs for 2 minutes
    trigger_ingestion = TriggerDagRunOperator(
        task_id="trigger_ingestion",
        trigger_dag_id="crypto_ingestion",
        wait_for_completion=False # Fire and forget so we can start Spark immediately
    )

    # 2. Submit Spark Job (via REST API)
    def submit_spark_job_func():
        import requests
        import json
        import time
        
        # 1. Submit the job
        url = "http://spark-master:6066/v1/submissions/create"
        headers = {'Content-Type': 'application/json;charset=UTF-8'}
        data = {
            "action": "CreateSubmissionRequest",
            "appArgs": [],
            "appResource": "file:/opt/spark-project/jobs/spark_stream.py",
            "clientSparkVersion": "3.5.0",
            "environmentVariables": {
                "SPARK_ENV_LOADED": "1"
            },
            "mainClass": "org.apache.spark.deploy.PythonRunner",
            "sparkProperties": {
                "spark.jars.packages": "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
                "spark.master": "spark://spark-master:7077",
                "spark.submit.deployMode": "cluster",
                "spark.driver.supervise": "false",
                "spark.app.name": "CryptoDataStreaming"
            }
        }
        
        print("Submitting Spark Job...")
        response = requests.post(url, headers=headers, data=json.dumps(data))
        print(f"Submission Response: {response.text}")
        
        if response.status_code != 200:
            raise Exception("Failed to submit spark job")
            
        res_json = response.json()
        if not res_json.get("success"):
             raise Exception(f"Submission failed: {res_json.get('message')}")
             
        submission_id = res_json.get("submissionId")
        print(f"Job Submitted with ID: {submission_id}")
        
        # 2. Let it run for 2 minutes
        print("Running for 120 seconds...")
        time.sleep(120)
        
        # 3. Kill the job
        print(f"Killing job {submission_id}...")
        kill_url = f"http://spark-master:6066/v1/submissions/kill/{submission_id}"
        kill_resp = requests.post(kill_url, headers=headers)
        print(f"Kill Response: {kill_resp.text}")

    submit_spark_job = PythonOperator(
        task_id="submit_spark_job",
        python_callable=submit_spark_job_func
    )
    
    # 3. Monitor / Verification
    # Runs after Spark job finishes (timeout)
    run_monitor = BashOperator(
        task_id="run_monitor",
        bash_command="python /opt/airflow/project/scripts/monitor_alerts.py"
    )

    # Dependencies: Trigger Ingestion and Spark run in parallel
    # Monitor runs after Spark finishes
    
    trigger_ingestion >> submit_spark_job >> run_monitor
