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

DEFAULT_UBUNTU_IMAGE = 'ubuntu/images/hvm-ssd/ubuntu-jammy-22.04-amd64-server-202306*'
DEFAULT_UBUNTU_ACCOUNT_ID = '099720109477'
AMQP_URL_FORMAT = "amqp://{}:{}@{}:5672/{}"

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
        self.security_group = 'Lithops-RabbitMQ'

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

        #  Create the security group if not exists
        if not 'security_group_id' in self.config:
            self._create_security_group()

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
    
    def _build_runtime_ami(self, image_name, script_file=None, extra_args=[]):
        logger.info(f'Building AWS AMI with name: {image_name}')

        #  If image not exists, create it
        aws_images = self.ec2_client.describe_images(Filters=[{'Name': 'name', 'Values': [image_name]}])['Images']
        if len(aws_images) > 0:
            if '--overwrite' in extra_args or '-o' in extra_args:
                logger.debug(f"Overwriting image {image_name}")
                self.delete_runtime(image_name, None)
            else:
                logger.debug(f"Image {image_name} already exists."
                             " Use '--overwrite' or '-o' if you want ot overwrite it")
                self.config['target_ami'] = aws_images[0]['ImageId']
                return

        #  Get the default image ID
        if 'target_ami' not in self.config:
            self._extract_ami(DEFAULT_UBUNTU_IMAGE, True)

        #  Create new instance to build the image
        build_vm = EC2Instance('building-image-' +image_name, self.config, self.ec2_client)

        #  Install actual python and lithops version
        user_data = config.BUILD_IMAGE_INIT_SCRIPT + \
            f"\nexport pyversion=python{sys.version_info[0]}.{sys.version_info[1]}" + \
            f"\nexport lithopsversion={__version__}"

        # Install all the needed packages (RabbitMQ, Python and Docker if needed)
        docker_prefix = "\nexport use_docker=True\n" if self.config.get('docker_image', False) else ""
        user_data += docker_prefix + config.BUILD_IMAGE_INSTALL_SCRIPT

        #  Include entry_point.py
        current_dir = os.path.dirname(os.path.realpath(__file__))
        entry_point_path = os.path.join(current_dir, 'entry_point.py')
        with open(entry_point_path, 'r') as f:
            entry_point_path = f.read()
            user_data = user_data + f"\necho '{entry_point_path}' > entry_point.py\n"

        #  Install all custom packages
        if script_file and os.path.isfile(script_file):
            logger.debug(f"Uploading local file '{script_file}' to VM image")
            with open(script_file, 'r') as f:
                script_file = f.read()
                user_data = user_data + script_file

        #  Include custom files in the image
        include_files = [arg for arg in extra_args if not arg.startswith(
            '-') and arg not in ['-i', '--include', extra_args[extra_args.index(arg)+1] if arg in ['-i', '--include'] else '']]
        if include_files:
            for src_dst_file in include_files:
                src_file, dst_file = src_dst_file.split(':')
                if os.path.isfile(src_file):
                    logger.debug(f"Uploading local file '{src_file}' to VM image in '{dst_file}'")
                    with open(src_file, 'r') as f:
                        src_file = f.read()
                        user_data = user_data + f"\necho '{src_file}' > {dst_file}\n"

        # Creating a custom AMQP URL for security (user_id:user_id@localhost:5672/iam_role)
        user_data = user_data + f"\nexport username={self.user_id}\n" + \
            f"export timeout={self.config['runtime_timeout']}\n" + \
            f"export vhost={self.config['iam_role']}\n" + config.BUILD_IMAGE_CONFIG_SCRIPT

        # Create the build VM and wait for it to be ready
        build_vm.create(user_data=user_data, server=True)

        logger.debug("Executing Lithops installation script. Be patient, this process can take up to 5 minutes")

        self.amqp_url = AMQP_URL_FORMAT.format(self.user_id, self.user_id, build_vm.public_ip, self.config['iam_role'])
        self.wait_build_configured(self.amqp_url)
        logger.debug("Lithops installation script finished")
        self.amqp_url = None

        #  Create an image from the actual VM
        self.ec2_client.create_image(
            InstanceId=build_vm.instance_id,
            Name=image_name,
            Description='Lithops Image'
        )

        logger.info("Starting VM image creation")
        logger.debug("Be patient, VM imaging can take up to 5 minutes")

        while True:
            images = self.ec2_client.describe_images(Filters=[{'Name': 'name', 'Values': [image_name]}])['Images']
            if len(images) > 0:
                logger.debug(f"VM Image is being created. Current status: {images[0]['State']}")
                if images[0]['State'] == 'available':
                    break
                if images[0]['State'] == 'failed':
                    raise Exception(f"Failed to create VM Image {image_name}")
            time.sleep(20)

        build_vm.delete()

        logger.info(f"VM Image created. Image ID: {images[0]['ImageId']}")

        self.config['target_ami'] = images[0]['ImageId']

    def _build_default_docker(self, docker_image_name, extra_args=[]):
        """
        Builds the default runtime
        """
        # Build default runtime using local dokcer
        dockerfile = "Dockefile.default-ec2-runtime"
        with open(dockerfile, 'w') as f:
            f.write(f"FROM python:{utils.CURRENT_PY_VERSION}-slim-buster\n")
            f.write(config.DOCKERFILE_DEFAULT)
        try:
            self._build_runtime_docker(docker_image_name, dockerfile, extra_args)
        finally:
            os.remove(dockerfile)

    def _build_runtime_docker(self, docker_image_name, dockerfile=None, extra_args=[]):
        logger.info(f'Building runtime {docker_image_name} from {dockerfile}')

        docker_path = utils.get_docker_path()

        #  Build the docker image
        assert os.path.isfile(dockerfile), f'Cannot locate "{dockerfile}"'
        cmd = f'{docker_path} build --platform=linux/amd64 -t {docker_image_name} -f {dockerfile} . ' + ' '.join(extra_args)

        try:
            entry_point = os.path.join(os.path.dirname(__file__), 'entry_point.py')
            utils.create_handler_zip(config.FH_ZIP_LOCATION, entry_point, 'entry_point.py')
            utils.run_command(cmd)
        finally:
            os.remove(config.FH_ZIP_LOCATION)

        #  Check if exists the docker credentials
        required_keys = ['docker_user', 'docker_password', 'docker_server']
        if not all(key in self.config for key in required_keys):
            missing_keys = ', '.join(key for key in required_keys if key not in self.config)
            raise Exception(f'Docker credentials ({missing_keys}) are needed to build custom Docker images')

        #  Login to the docker registry
        cmd = f'{docker_path} login -u {self.config.get("docker_user")} --password-stdin {self.config.get("docker_server")}'
        utils.run_command(cmd, input=self.config.get("docker_password"))

        #  Push the image to the registry
        logger.debug(f'Pushing runtime {docker_image_name} to container registry')
        if utils.is_podman(docker_path):
            cmd = f'{docker_path} push {docker_image_name} --format docker --remove-signatures'
        else:
            cmd = f'{docker_path} push {docker_image_name}'
        utils.run_command(cmd)

    def build_runtime(self, image_name, script_file=None, extra_args=[]):
        """
        Builds a new Docker image or AMI with the runtime
        """
        # Check if user wants to build a docker image or an AMI
        if '--docker' in extra_args:
            extra_args.remove('--docker')
            if script_file:
                self._build_runtime_docker(image_name, script_file, extra_args)
            else:
                self._build_default_docker(image_name, extra_args)

        #  Build an AMI
        else:
            self._build_runtime_ami(image_name, script_file, extra_args)

        logger.debug('Building done!')

    def deploy_runtime(self, image_name, memory, timeout):
        """
        Deploys a new runtime
        """
        default_image_name = self._get_default_runtime_image_name()
        if image_name == default_image_name:
            self.build_runtime(image_name)

        logger.info(f"Deploying image: {image_name}")

        # Create a worker to generate the runtime metadata
        runtime_meta = self._generate_runtime_meta(image_name)

        return runtime_meta

    def delete_runtime(self, image_name, memory, version=__version__):
        """
        Deletes a runtime
        """
        images = self.ec2_client.describe_images(Filters=[{'Name': 'name', 'Values': [image_name]}])['Images']

        if len(images) > 0:
            image_id = images[0]['ImageId']
            logger.debug(f"Deleting existing VM Image '{image_name}'")
            self.ec2_client.deregister_image(ImageId=image_id)
            images = self.ec2_client.describe_images(Filters=[{'Name': 'name', 'Values': [image_name]}])['Images']
            while len(images) > 0:
                time.sleep(2)
                images = self.ec2_client.describe_images(Filters=[{'Name': 'name', 'Values': [image_name]}])['Images']
            logger.debug(f"VM Image '{image_name}' successfully deleted")

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

        # Delete the security group
        self.ec2_client.delete_security_group(GroupId=self.config['security_group_id'])
        logger.debug('Security Group deleted')

    def list_runtimes(self, image_name='all'):
        """
        List all the runtimes
        return: list of tuples (image_name, memory)
        """
        logger.debug('Listing runtimes')
        logger.debug('Note that this backend does not manage runtimes')
        return []

    def _start_rabbit_server(self, image_name):
        self._extract_ami(image_name)

        server_vm = EC2Instance("lithops-rabbitserver-" + self.user_id[-4:], self.config, self.ec2_client)

        # Creating a custom AMQP URL for security (user_id:user_id@localhost:5672/iam_role)
        user_data = config.BUILD_IMAGE_INIT_SCRIPT + \
            f"\nexport username={self.user_id}\n" + \
            f"export timeout={self.config['runtime_timeout']}\n" + \
            f"export vhost={self.config['iam_role']}\n" + config.BUILD_IMAGE_CONFIG_SCRIPT

        server_vm.create(user_data=user_data, server=True)
        logger.debug("Starting RabbitMQ server. Be patient, this process can take up to 1 minutes")

        self.amqp_url = AMQP_URL_FORMAT.format(self.user_id, self.user_id, server_vm.public_ip, self.config['iam_role'])
        self.wait_build_configured(self.amqp_url)
        logger.debug("RabbitMQ server started successfully")

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
        if docker_image:
            # Check if docker_user and docker_password are set
            required_keys = ['docker_user', 'docker_password', 'docker_server']

            if not all(key in self.config for key in required_keys):
                missing_keys = ', '.join(key for key in required_keys if key not in self.config)
                raise Exception(f'Docker credentials ({missing_keys}) are needed to build custom Docker images')

            user_data = f"""#!/bin/bash
                echo {self.config['docker_password']} | docker login -u {self.config['docker_user']} --password-stdin {self.config['docker_server']}
                sudo docker run --name lithops-rabbitmq {self.config['docker_server']}/{docker_image} python entry_point.py 'start_rabbitmq' {utils.dict_to_b64str(payload)}
                sudo shutdown -h now
            """
        else:
            user_data = f"""#!/bin/bash
                source lithops/bin/activate
                python entry_point.py 'start_rabbitmq' {utils.dict_to_b64str(payload)}
            """

        worker = EC2Instance(name, self.config, self.ec2_client)

        try:
            worker.create(user_data=user_data)
        except Exception as e:
            self.thread_errors.append(e)
            return

        self.workers.append(worker)

    def _create_security_group(self):
        """
        Creates a new Security group
        """
        logger.debug(f'Creating Security Group in VPC {self.security_group}')

        # Check if the security group already exists
        security_groups = self.ec2_client.describe_security_groups(
            Filters=[
                {
                    'Name': 'group-name',
                    'Values': [self.security_group]
                },
                {
                    'Name': 'description',
                    'Values': [self.security_group]
                }
            ]
        )

        if len(security_groups['SecurityGroups']) > 0:
            logger.debug(f"Security Group {self.security_group} already exists")
            self.config['security_group_id'] = security_groups['SecurityGroups'][0]['GroupId']
            return

        # If not exists, create it
        else:
            response = self.ec2_client.create_security_group(
                GroupName=self.security_group,
                Description=self.security_group,
            )

            self.ec2_client.authorize_security_group_ingress(
                GroupId=response['GroupId'],
                IpPermissions=[
                    # RabbitMQ ports
                    {'IpProtocol': 'tcp',
                        'FromPort': 15672,
                        'ToPort': 15672,
                        'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                    {'IpProtocol': 'tcp',
                        'FromPort': 4369,
                        'ToPort': 4369,
                        'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                    {'IpProtocol': 'tcp',
                        'FromPort': 5671,
                        'ToPort': 5672,
                        'IpRanges': [{'CidrIp': '0.0.0.0/0'}]},
                ]
            )

            self.config['security_group_id'] = response['GroupId']

    def _extract_ami(self, image_name, default=False):
        owners = [DEFAULT_UBUNTU_ACCOUNT_ID] if default else ["self"]
        images = self.ec2_client.describe_images(
            Filters=[{'Name': 'name', 'Values': [image_name]}],
            Owners=owners
        )['Images']

        if len(images) > 0:
            self.config['target_ami'] = images[0]['ImageId']
        else:
            raise Exception(f"Image {image_name} not found")

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
                worker.public_ip = instance.get('PublicIpAddress', None)
                worker.spot_instance = instance.get('InstanceLifecycle', None) == 'spot'
                worker.target_ami = instance['ImageId']
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

        #  Get the Rabbit server ampq url
        if not self.amqp_url:
            # Check if the rabbit server is already started
            for worker in self.workers:
                if 'lithops-rabbitserver-' in worker.name:
                    logger.info(f"Rabbit Server found: {worker.public_ip}")

                    self.amqp_url = AMQP_URL_FORMAT.format(self.user_id, self.user_id, worker.public_ip, self.config['iam_role'])
                    self.config['target_ami'] = worker.target_ami

                    self.workers.remove(worker)
                    break

            #  Rabbit server not found, start it
            else:
                self._start_rabbit_server(image_name)

        # Rabbit server already started
        elif self.config.get('target_ami', None) is None:
            self._extract_ami(image_name)

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

    def _generate_runtime_meta(self, image_name):
        runtime_name = self._format_job_name(image_name, 128)
        job_name = f'{runtime_name}-meta'

        logger.info(f"Extracting metadata from: {image_name}")

        payload = copy.deepcopy(self.internal_storage.storage.config)
        payload['runtime_name'] = runtime_name
        payload['log_level'] = logger.getEffectiveLevel()

        self._extract_ami(image_name)

        worker = EC2Instance(job_name, self.config, self.ec2_client)
        docker_image = self.config.get('docker_image', False)
        if docker_image:
            # Check if docker_user and docker_password are set
            required_keys = ['docker_user', 'docker_password', 'docker_server']

            if not all(key in self.config for key in required_keys):
                missing_keys = ', '.join(key for key in required_keys if key not in self.config)
                raise Exception(f'Docker credentials ({missing_keys}) are needed to build custom Docker images')

            user_data = f"""#!/bin/bash
                echo {self.config['docker_password']} | docker login -u {self.config['docker_user']} --password-stdin {self.config['docker_server']}
                sudo docker run --name lithops-rabbitmq {self.config['docker_server']}/{docker_image} python entry_point.py 'get_metadata' {utils.dict_to_b64str(payload)}
            """
        else:
            user_data = f"""#!/bin/bash
                source lithops/bin/activate
                python entry_point.py 'get_metadata' {utils.dict_to_b64str(payload)}
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

    def wait_build_configured(self, amqp_url):
        """
        Wait until the VM send the confirmation message as the configuration is finished
        """
        while True:
            try:
                params = pika.URLParameters(amqp_url)
                connection = pika.BlockingConnection(params)
                channel = connection.channel()
                break  # If connection is established, break the loop
            except pika.exceptions.AMQPConnectionError:
                time.sleep(0.5)  # If connection fails, sleep and retry

        channel.queue_declare(queue='build_confirmation', durable=True)

        def callback(ch, method, properties, body):
            ch.stop_consuming()  # Stop consuming messages

        channel.basic_consume(queue='build_confirmation', on_message_callback=callback, auto_ack=True)
        channel.start_consuming()


class EC2Instance:
    def __init__(self, name, ec2_config, ec2_client):
        """
        Initialize a EC2Instance instance
        VMs can have master role, this means they will have a public IP address
        """
        self.name = name.lower()
        self.config = ec2_config

        self.worker_instance_type = self.config['worker_instance_type']
        self.server_instance_type = self.config['server_instance_type']
        self.spot_instance = self.config['request_spot_instances']
        self.ssh_key_name = self.config.get('ssh_key_name', None)
        self.availability_zone = self.config.get('availability_zone', None)
        self.memory_size = self.config.get('memory_size', None)

        self.ec2_client = ec2_client

        self.instance_id = None
        self.target_ami = None
        self.public_ip = None
        self.cpu_count = 0

    def create(self, user_data=None, server=False):
        """
        Creates a new VM instance
        """
        LaunchSpecification = {
            "ImageId": self.config['target_ami'],
            "InstanceType": self.server_instance_type if server else self.worker_instance_type,
            "IamInstanceProfile": {'Name': self.config['iam_role']},
            "Monitoring": {'Enabled': False},
            "MinCount": 1,
            "MaxCount": 1,
            "TagSpecifications": [{"ResourceType": "instance", "Tags": [{'Key': 'Name', 'Value': self.name}]}],
        }

        LaunchSpecification['NetworkInterfaces'] = [{
            'AssociatePublicIpAddress': True,
            'DeviceIndex': 0,
            'Groups': [self.config['security_group_id']]
        }]

        if self.memory_size:
            LaunchSpecification['BlockDeviceMappings'] = [{
                'DeviceName': '/dev/sda1',
                'Ebs': {
                    'VolumeSize': self.memory_size,
                    'VolumeType': 'gp2'
                }
            }]

        if self.ssh_key_name:
            LaunchSpecification['KeyName'] = self.ssh_key_name

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
        self.target_ami = instance_data['ImageId']
        self.cpu_count = instance_data['CpuOptions']['CoreCount']

        #  Get the public IP only if the instance is a server
        if server:
            self.public_ip = self.get_public_ip()

        logger.debug(f"VM instance {self.name} created successfully ")

    def delete(self):
        """
        Deletes the VM instance and the associated volume
        """
        logger.debug(f"Deleting VM instance {self.name} ({self.instance_id})")

        self.ec2_client.terminate_instances(InstanceIds=[self.instance_id])

        self.instance_id = None
        self.public_ip = '0.0.0.0'

    def get_public_ip(self):
        """
        Get the public IP of the VM instance
        """
        waiter = self.ec2_client.get_waiter('instance_running')
        waiter.wait(InstanceIds=[self.instance_id])

        instance_data = self.ec2_client.describe_instances(InstanceIds=[self.instance_id])[
            'Reservations'][0]['Instances'][0]
        return instance_data.get('PublicIpAddress')
