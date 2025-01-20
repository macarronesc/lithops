#
# (C) Copyright Cloudlab URV 2025
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import pika
import os
import sys
import uuid
import json
import logging
import time
from functools import partial
from multiprocessing import Value, cpu_count
from threading import Thread, Timer

from lithops.version import __version__
from lithops.utils import setup_lithops_logger, b64str_to_dict
from lithops.worker import function_handler
from lithops.worker.utils import get_runtime_metadata
from lithops.constants import JOBS_PREFIX
from lithops.storage.storage import InternalStorage

from lithops.standalone.utils import StandaloneMode

logger = logging.getLogger("lithops.worker")

def shutdown():
    logger.info("Shutting down the worker")
    os.system("sudo shutdown -h now")
    os._exit(0)

def extract_runtime_meta(payload):
    logger.info(f"Lithops v{__version__} - Generating metadata")

    runtime_meta = get_runtime_metadata()

    internal_storage = InternalStorage(payload)
    status_key = "/".join([JOBS_PREFIX, payload["runtime_name"] + ".meta"])
    logger.info(f"Runtime metadata key {status_key}")
    dmpd_response_status = json.dumps(runtime_meta)
    internal_storage.put_data(status_key, dmpd_response_status)


def run_job_k8s_rabbitmq(payload):
    logger.info(f"Lithops v{__version__} - Starting EC2 Job execution")

    act_id = str(uuid.uuid4()).replace("-", "")[:12]
    os.environ["__LITHOPS_ACTIVATION_ID"] = act_id
    os.environ["__LITHOPS_BACKEND"] = "k8s_rabbitmq"

    function_handler(payload)
    with running_jobs.get_lock():
        running_jobs.value += len(payload["call_ids"])

    logger.info("Finishing EC2 Job execution")

    if mode == StandaloneMode.CREATE.value and running_jobs.value == cpus_machine:
        shutdown()

def callback_work_queue(ch, method, properties, body):
    """Callback to receive the payload and run the jobs"""
    global timeout_timer 
    
    logger.info("Call from lithops received.")

    # Cancel the shutdown timer
    timeout_timer.cancel()

    message = json.loads(body)
    tasks = message["total_calls"]

    # If there are more tasks than cpus in the pod, we need to send a new message
    if tasks <= running_jobs.value:
        processes_to_start = tasks
    else:
        if running_jobs.value == 0:
            logger.info("All cpus are busy. Waiting for a cpu to be free")
            ch.basic_nack(delivery_tag=method.delivery_tag)
            time.sleep(0.5)
            return

        processes_to_start = running_jobs.value

        message_to_send = message.copy()
        message_to_send["total_calls"] = tasks - running_jobs.value
        message_to_send["call_ids"] = message_to_send["call_ids"][running_jobs.value:]
        message_to_send["data_byte_ranges"] = message_to_send["data_byte_ranges"][running_jobs.value:]

        message["total_calls"] = running_jobs.value
        message["call_ids"] = message["call_ids"][:running_jobs.value]
        message["data_byte_ranges"] = message["data_byte_ranges"][:running_jobs.value]

        ch.basic_publish(
            exchange="",
            routing_key="task_queue",
            body=json.dumps(message_to_send),
            properties=pika.BasicProperties(
                delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
            ))

    logger.info(f"Starting {processes_to_start} processes")

    message["worker_processes"] = running_jobs.value
    with running_jobs.get_lock():
        running_jobs.value -= processes_to_start

    Thread(target=run_job_k8s_rabbitmq, args=([message])).start()

    ch.basic_ack(delivery_tag=method.delivery_tag)

    # Start a new shutdown timer
    timeout_timer = Timer(timeout_client, lambda: shutdown()) 
    timeout_timer.start()

def start_rabbitmq_listening(payload):
    global running_jobs
    global timeout_timer 
    global timeout_client
    global cpus_machine
    global mode

    # Connect to rabbitmq
    try:
        params = pika.URLParameters(payload["amqp_url"])
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue="task_queue", durable=True)
        channel.basic_qos(prefetch_count=1)
    except Exception as e:
        shutdown()

    # Shared variable to track completed jobs
    running_jobs = Value("i", cpu_count())
    cpus_machine = cpu_count()

    # Set the mode and timeout from the client
    mode = payload["mode"]
    timeout_client = payload["timeout"]

    # Start listening to the new job
    channel.basic_consume(queue="task_queue", on_message_callback=callback_work_queue)

    # Start the shutdown timer
    timeout_timer = Timer(timeout_client, lambda: shutdown())
    timeout_timer.start() 

    logger.info("Listening to rabbitmq...")
    channel.start_consuming()


if __name__ == "__main__":
    action = sys.argv[1]
    encoded_payload = sys.argv[2]

    payload = b64str_to_dict(encoded_payload)
    setup_lithops_logger(payload.get("log_level", "INFO"))

    switcher = {
        "get_metadata": partial(extract_runtime_meta, payload),
        "start_rabbitmq": partial(start_rabbitmq_listening, payload)
    }

    func = switcher.get(action, lambda: "Invalid command")
    func()
