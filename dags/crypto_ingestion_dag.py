import json
import logging
import time
from datetime import datetime

import websocket
from airflow import DAG
from airflow.operators.python import PythonOperator
from kafka import KafkaProducer

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 12, 27),
}

def stream_data():
    """
    Stream data from Coinbase WebSocket to Kafka
    """
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    
    def on_message(ws, message):
        try:
            data = json.loads(message)
            if 'price' in data and 'product_id' in data:
                # Format for Kafka
                payload = {
                    'symbol': data['product_id'],
                    'price': float(data['price']),
                    'volume': float(data.get('last_size', 0)),
                    'timestamp': data.get('time')
                }
                producer.send('crypto_trades', json.dumps(payload).encode('utf-8'))
                logging.info(f"Published: {payload}")
        except Exception as e:
            logging.error(f"Error processing message: {e}")

    def on_error(ws, error):
        logging.error(f"WebSocket Error: {error}")

    def on_close(ws, close_status_code, close_msg):
        logging.info("WebSocket Closed")

    def on_open(ws):
        logging.info("WebSocket Opened")
        subscribe_message = {
            "type": "subscribe",
            "product_ids": ["BTC-USD", "ETH-USD"],
            "channels": ["ticker"]
        }
        ws.send(json.dumps(subscribe_message))

    # Connect and run
    # For a real pipeline, this should be robust with reconnection logic.
    # For this Airflow task, we will run it for a set duration or until stopped.
    # Beacuse websocket.run_forever blocking, we can wrap it or just let it run.
    # Given Airflow tasks should finish, we can't run forever here unless we use a sensor or external service.
    # We will run for 2 minutes as a batch ingestion simulation for this demo.
    
    ws = websocket.WebSocketApp("wss://ws-feed.exchange.coinbase.com",
                              on_open=on_open,
                              on_message=on_message,
                              on_error=on_error,
                              on_close=on_close)
    
    # Run for a fixed time is tricky with run_forever. 
    # Instead, we will use a loop with manual recv/ping if we want control, 
    # OR just let it run and relying on Airflow execution timeout if we wanted a long runner.
    # But for a robust "Pipeline", I'll use a loop checking time in on_message to close.
    
    start_time = time.time()
    
    def on_message_timed(ws, message):
        # Check duration
        if time.time() - start_time > 120: # Run for 2 minutes
            ws.close()
            return
        on_message(ws, message)

    ws.on_message = on_message_timed
    ws.run_forever()

with DAG(
    dag_id="crypto_ingestion",
    default_args=default_args,
    schedule="@daily", # or @continuous if using newer Airflow features, but @daily is fine for testing
    catchup=False
) as dag:
    
    streaming_task = PythonOperator(
        task_id="stream_crypto_data",
        python_callable=stream_data
    )
