#
# (C) Copyright Cloudlab URV 2021
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

import os
import sys
import pika
import hashlib
import json
import logging
import copy
import time
import urllib3
import threading

import boto3
import botocore

from lithops import utils
from lithops.version import __version__
from lithops.constants import COMPUTE_CLI_MSG, JOBS_PREFIX

from . import config

logger = logging.getLogger(__name__)
urllib3.disable_warnings()


class AWSEC2Backend:
    """
    A wrap-up around AWS EC2 backend.
    """

    def __init__(self, config, internal_storage):
        logger.debug("Creating AWS EC2 client")
        self.name = 'aws_ec2'
        self.type = utils.BackendType.BATCH.value
        self.config = config
        self.internal_storage = internal_storage

        self.mode = self.config['exec_mode']
        self.region_name = self.config['region']

        self.aws_session = boto3.Session(
            aws_access_key_id=config.get('access_key_id'),
            aws_secret_access_key=config.get('secret_access_key'),
            aws_session_token=config.get('session_token'),
            region_name=self.region_name
        )

        self.ec2_client = self.aws_session.client(
            'ec2', config=botocore.client.Config(
                user_agent_extra=self.config['user_agent']
            )
        )

        self.user_id = self.aws_session.get_credentials().access_key.lower()
        self.workers = []
        self.amqp_url = self.config.get('amqp_url', False)
        self.thread_errors = []

        msg = COMPUTE_CLI_MSG.format('AWS EC2')
        logger.info(f"{msg} - Region: {self.region_name}")

    def _format_job_name(self, runtime_name, runtime_memory, version=__version__):
        name = f'{runtime_name}-{runtime_memory}-{version}-{self.user_id[-4:]}'
        name_hash = hashlib.sha1(name.encode()).hexdigest()[:10]

        return f'lithops-worker-{version.replace(".", "")}-{name_hash}'

    def _get_default_runtime_image_name(self):
        """
        Generates the default runtime image name
        """
        py_version = utils.CURRENT_PY_VERSION.replace('.', '')
        return f'lithops-ec2-default-v{py_version}-lv{__version__}'
    
    def build_runtime(self, image_name, script_file=None, extra_args=[]):
        """
        Builds a new Docker image or AMI with the runtime
        """
        logger.debug('No build needed for AWS EC2 backend')

    def deploy_runtime(self, image_name, memory, timeout):
        """
        Deploys a new runtime
        """
        logger.info(f"Deploying image: {image_name}")
        return self._generate_runtime_meta(image_name)

    def delete_runtime(self, image_name, memory, version=__version__):
        """
        Deletes a runtime
        """
        logger.debug('No runtime deletion needed for AWS EC2 backend')

    def list_runtimes(self, image_name='all'):
        """
        List all the runtimes
        return: list of tuples (image_name, memory)
        """
        logger.debug('Listing runtimes')
        logger.debug('Note that this backend does not manage runtimes')
        return []

    def clean(self, **kwargs):
        """
        Deletes all jobs
        """
        logger.info('Cleaning all Lithops resources in AWS EC2 backend')

        workers = self._get_workers()
        if workers:
            for worker in workers:
                worker.delete()

            # Wait for all workers to be deleted
            while self.ec2_client.describe_instances(Filters=[{'Name': 'instance-state-name', 'Values': ['running', 'shutting-down']}])['Reservations']:
                time.sleep(2)

            logger.info('All EC2 VMs deleted')

    # TODO: Change docker_user and docker_password to AWS CodeBuild
    def create_worker(self, name, cpu):
        """
        Creates a new worker VM instance
        """
        payload = {
            'log_level': 'DEBUG',
            'amqp_url': self.amqp_url,
            'timeout': self.config['runtime_timeout'],
            'mode': self.mode,
        }

        docker_image = self.config.get('docker_image', False)
        # Check if docker_user and docker_password are set
        required_keys = ['docker_user', 'docker_password', 'docker_server']

        if not all(key in self.config for key in required_keys):
            missing_keys = ', '.join(key for key in required_keys if key not in self.config)
            raise Exception(f'Docker credentials ({missing_keys}) are needed to build custom Docker images')

        user_data = f"""#!/bin/bash
            echo {self.config['docker_password']} | docker login -u {self.config['docker_user']} --password-stdin {self.config['docker_server']}
            sudo docker run --name lithops-rabbitmq {self.config['docker_server']}/{docker_image} python -m lithops.serverless.backends.aws_ec2_rabbit.entry_point 'start_rabbitmq' {utils.dict_to_b64str(payload)}
            sudo shutdown -h now
        """

        worker = EC2Instance(name, self.config, self.ec2_client)

        try:
            worker.create(user_data=user_data)
        except Exception as e:
            self.thread_errors.append(e)
            return

        self.workers.append(worker)

    def _get_workers(self):
        """
        Get the machines working on AWS EC2
        """
        workers = []

        reservations = self.ec2_client.describe_instances(
            Filters=[
                {'Name': 'instance-state-name', 'Values': ['running']},
                {'Name': 'tag:Name', 'Values': ['lithops-*']}
            ]
        )['Reservations']

        for reservation in reservations:
            for instance in reservation['Instances']:
                worker = EC2Instance(instance['Tags'][0]['Value'], self.config, self.ec2_client)
                worker.instance_id = instance['InstanceId']
                worker.spot_instance = instance.get('InstanceLifecycle', None) == 'spot'
                worker.cpu_count = instance['CpuOptions']['CoreCount']
                workers.append(worker)

        return workers

    def _get_instance_cpus(self, instance_type):
        """
        Gets the instance number of cpu
        """
        instance_info = self.ec2_client.describe_instance_types(InstanceTypes=[instance_type])['InstanceTypes'][0]
        return instance_info['VCpuInfo']['DefaultVCpus']

    def invoke(self, image_name, runtime_memory, job_payload):
        """
        Invoke -- return information about this invocation
        For array jobs only remote_invocator is allowed
        """
        # Get all the workers
        self.workers = self._get_workers()
        cpu_count = 0

        #  Get number of cpus
        for worker in self.workers:
            if f'lithops-worker-{self.user_id[-4:]}' in worker.name:
                cpu_count += worker.cpu_count

        #  Get the number of cpus of the worker type
        cpus_worker_type = self._get_instance_cpus(self.config['worker_instance_type'])

        # Creating necessary workers
        if cpu_count < job_payload['total_calls']:
            logger.info("Not enough cpus to execute the job. Creating new workers")

            # Calculate the number of workers needed
            new_cpus = job_payload['total_calls'] - cpu_count
            num_workers = new_cpus // cpus_worker_type + (new_cpus % cpus_worker_type > 0)

            #  Create the workers
            cpu_count = cpus_worker_type * num_workers
            threads = [threading.Thread(target=self.create_worker, args=(
                f'lithops-worker-{self.user_id[-4:]}-{i}', cpus_worker_type)) for i in range(num_workers)]
            for thread in threads:
                thread.start()

            # Wait for all threads to complete
            for thread in threads:
                thread.join()

            if self.thread_errors:
                for error in self.thread_errors:
                    logger.error(error)
                raise Exception("Failed to create workers")

        # Calculate the number of packages to send to each worker
        times, res = divmod(job_payload['total_calls'], cpus_worker_type)

        #  Send the tasks to the queue
        try:
            params = pika.URLParameters(self.amqp_url)
            connection = pika.BlockingConnection(params)
            channel = connection.channel()

            for i in range(times + (1 if res != 0 else 0)):
                num_tasks = cpus_worker_type if i < times else res
                payload_edited = job_payload.copy()

                start_index = i * cpus_worker_type
                end_index = start_index + num_tasks

                payload_edited['call_ids'] = payload_edited['call_ids'][start_index:end_index]
                payload_edited['data_byte_ranges'] = payload_edited['data_byte_ranges'][start_index:end_index]
                payload_edited['total_calls'] = num_tasks

                channel.basic_publish(
                    exchange='',
                    routing_key='task_queue',
                    body=json.dumps(payload_edited),
                    properties=pika.BasicProperties(
                        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
                    ))

        except pika.exceptions.AMQPConnectionError as e:
            raise Exception(
                f"Failed to connect to RabbitMQ server. This could be due to the server not being started or it has old credentials")

        activation_id = f'lithops-{job_payload["job_key"].lower()}'

        return activation_id

    # TODO: Improve it
    def _generate_runtime_meta(self, image_name):
        runtime_name = self._format_job_name(image_name, 128)
        job_name = f'{runtime_name}-meta'

        logger.info(f"Extracting metadata from: {image_name}")

        payload = copy.deepcopy(self.internal_storage.storage.config)
        payload['runtime_name'] = runtime_name
        payload['log_level'] = logger.getEffectiveLevel()

        worker = EC2Instance(job_name, self.config, self.ec2_client)
        docker_image = self.config.get('docker_image', False)
        # Check if docker_user and docker_password are set
        required_keys = ['docker_user', 'docker_password', 'docker_server']

        if not all(key in self.config for key in required_keys):
            missing_keys = ', '.join(key for key in required_keys if key not in self.config)
            raise Exception(f'Docker credentials ({missing_keys}) are needed to build custom Docker images')

        user_data = f"""#!/bin/bash
            echo {self.config['docker_password']} | docker login -u {self.config['docker_user']} --password-stdin {self.config['docker_server']}
            sudo docker run --name lithops-rabbitmq {self.config['docker_server']}/{docker_image} python -m lithops.serverless.backends.aws_ec2_rabbit.entry_point 'get_metadata' {utils.dict_to_b64str(payload)}
        """

        worker.create(user_data=user_data)

        logger.debug("Waiting for runtime metadata")

        start = time.time()
        while (time.time() - start < 360):
            try:
                data_key = '/'.join([JOBS_PREFIX, runtime_name + '.meta'])
                json_str = self.internal_storage.get_data(key=data_key)
                runtime_meta = json.loads(json_str.decode("ascii"))
                self.internal_storage.del_data(key=data_key)

                if not runtime_meta or 'preinstalls' not in runtime_meta:
                    raise Exception(f'Failed getting runtime metadata: {runtime_meta}')

                #  Stop the worker
                worker.delete()

                logger.debug("Runtime metadata generated successfully")
                return runtime_meta

            except Exception as e:
                pass
        raise TimeoutError(f'Unable to extract metadata from the runtime')

    def get_runtime_key(self, image_name, runtime_memory, version=__version__):
        """
        Method that creates and returns the runtime key.
        Runtime keys are used to uniquely identify runtimes within the storage,
        in order to know which runtimes are installed and which not.
        """
        jobdef_name = self._format_job_name(image_name, 256, version)
        runtime_key = os.path.join(self.name, version, self.region_name, jobdef_name)

        return runtime_key

    def get_runtime_info(self):
        """
        Method that returns all the relevant information about the runtime set
        in config
        """
        runtime_info = {
            'runtime_name': self.config.get('runtime', self._get_default_runtime_image_name()),
            'runtime_cpu': 1,
            'runtime_memory': None,
            'runtime_timeout': self.config['runtime_timeout'],
            'max_workers': self.config['max_workers'],
        }

        return runtime_info

class EC2Instance:
    def __init__(self, name, ec2_config, ec2_client):
        """
        Initialize a EC2Instance instance
        VMs can have master role, this means they will have a public IP address
        """
        self.name = name.lower()
        self.config = ec2_config

        self.worker_instance_type = self.config['worker_instance_type']
        self.spot_instance = self.config['request_spot_instances']
        self.target_ami = self.config['target_ami']
        self.availability_zone = self.config.get('availability_zone', None)
        self.memory_size = self.config.get('memory_size', None)

        self.ec2_client = ec2_client

        self.instance_id = None
        self.cpu_count = 0

    def create(self, user_data=None):
        """
        Creates a new VM instance
        """
        LaunchSpecification = {
            "ImageId": self.target_ami,
            "InstanceType": self.worker_instance_type,
            "IamInstanceProfile": {'Name': self.config['iam_role']},
            "Monitoring": {'Enabled': False},
            "MinCount": 1,
            "MaxCount": 1,
            "TagSpecifications": [{"ResourceType": "instance", "Tags": [{'Key': 'Name', 'Value': self.name}]}],
        }

        LaunchSpecification['NetworkInterfaces'] = [{
            'AssociatePublicIpAddress': True,
            'DeviceIndex': 0,
        }]

        if self.memory_size:
            LaunchSpecification['BlockDeviceMappings'] = [{
                'DeviceName': '/dev/sda1',
                'Ebs': {
                    'VolumeSize': self.memory_size,
                    'VolumeType': 'gp2'
                }
            }]

        if user_data:
            LaunchSpecification['UserData'] = user_data

        if self.availability_zone:
            LaunchSpecification['Placement'] = {
                'AvailabilityZone': self.availability_zone
            }

        if self.spot_instance:
            logger.debug(f"Creating new VM instance {self.name} (Spot)")

            LaunchSpecification['InstanceMarketOptions'] = {
                'MarketType': 'spot',
                'SpotOptions': {
                    'SpotInstanceType': 'one-time',
                    'InstanceInterruptionBehavior': 'terminate'
                }
            }
        else:
            logger.debug(f"Creating new VM instance {self.name}")

            LaunchSpecification["InstanceInitiatedShutdownBehavior"] = 'terminate'

        resp = self.ec2_client.run_instances(**LaunchSpecification)

        instance_data = resp['Instances'][0]
        self.instance_id = instance_data['InstanceId']
        self.cpu_count = instance_data['CpuOptions']['CoreCount']

        logger.debug(f"VM instance {self.name} created successfully ")

    def delete(self):
        """
        Deletes the VM instance and the associated volume
        """
        logger.debug(f"Deleting VM instance {self.name} ({self.instance_id})")

        self.ec2_client.terminate_instances(InstanceIds=[self.instance_id])
