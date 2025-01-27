#
# (C) Copyright Cloudlab URV 2020
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

#  Script to install RabbitMQ and Lithops in the VM
BUILD_IMAGE_INIT_SCRIPT = """#!/bin/bash
sudo echo "\$nrconf{restart} = 'a';" | sudo tee -a /etc/needrestart/needrestart.conf
"""

BUILD_IMAGE_INSTALL_SCRIPT = """
sudo apt-get update -y

# Install RabbitMQ
sudo apt-get install rabbitmq-server -y --fix-missing

# Check if I need to install docker
if [ "$use_docker" = "True" ] 
then
    # Add Docker repository
    sudo apt install apt-transport-https ca-certificates curl software-properties-common -y
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo apt-key add -
    sudo add-apt-repository "deb [arch=amd64] https://download.docker.com/linux/ubuntu jammy stable" -y

    # Install Docker
    sudo apt-get install docker-ce -y

    # Add user to docker group
    sudo usermod -aG docker ${USER}
fi

# Install python
sudo add-apt-repository ppa:deadsnakes/ppa -y
sudo apt-get install $pyversion python3-virtualenv -y
virtualenv -p /usr/bin/$pyversion --download lithops
source lithops/bin/activate
pip install setuptools --upgrade
pip install pika lithops[all]==$lithopsversion
"""

BUILD_IMAGE_CONFIG_SCRIPT = """
source lithops/bin/activate
sudo service rabbitmq-server start
sudo update-rc.d rabbitmq-server defaults
sudo rabbitmq-plugins enable rabbitmq_management

sudo rabbitmqctl add_user $username $username
sudo rabbitmqctl add_vhost $vhost
sudo rabbitmqctl set_permissions -p $vhost $username ".*" ".*" ".*"

sudo tee send_confirmation.py > /dev/null <<EOF
import pika
import os

username = os.getenv('username')
vhost = os.getenv('vhost')
timeout_client = int(os.getenv('timeout'))

params = pika.URLParameters(f'amqp://{username}:{username}@localhost:5672/{vhost}')
connection = pika.BlockingConnection(params)
channel = connection.channel()
channel.queue_declare(queue='build_confirmation', durable=True)

# Send confirmation of the rabbit server
channel.basic_publish(
    exchange='',
    routing_key='build_confirmation',
    body="",
    properties=pika.BasicProperties(
        delivery_mode=pika.spec.PERSISTENT_DELIVERY_MODE
    ))
connection.close()
EOF

python send_confirmation.py
"""

DEFAULT_CONFIG_KEYS = {
    'server_instance_type': 't2.micro',
    'worker_instance_type': 't2.micro',  # 't2.medium',
    'request_spot_instances': True,
    'max_workers': 100,
    'runtime_memory': 512,
    'worker_processes': 1,
    'runtime_timeout': 600,  # Default: 10 minutes
    'exec_mode': 'reuse',
    'docker_server': 'docker.io'
}

FH_ZIP_LOCATION = os.path.join(os.getcwd(), 'lithops_ec2.zip')

DOCKERFILE_DEFAULT = """
RUN apt-get update && apt-get install -y zip \
        && apt-get clean \
        && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade --ignore-installed setuptools six pip \
    && pip install --upgrade --no-cache-dir --ignore-installed \
        pika \
        boto3 \
        ibm-cloud-sdk-core \
        ibm-cos-sdk \
        requests \
        panda \
        numpy \
        cloudpickle \
        ps-mem \
        tblib \
        psutil

# Copy Lithops proxy and lib to the container image.
WORKDIR /lithops

COPY lithops_ec2.zip .
RUN unzip lithops_ec2.zip && rm lithops_ec2.zip
"""


def load_config(config_data):
    if not config_data['aws_ec2_rabbit']:
        raise Exception("'aws_ec2_rabbit' section is mandatory in the configuration")

    if 'aws' not in config_data:
        config_data['aws'] = {}

    config_data['aws_ec2_rabbit'].update(config_data['aws'])

    for key in DEFAULT_CONFIG_KEYS:
        if key not in config_data['aws_ec2_rabbit']:
            config_data['aws_ec2_rabbit'][key] = DEFAULT_CONFIG_KEYS[key]

    if 'iam_role' not in config_data['aws_ec2_rabbit']:
        raise Exception(f"'iam_role' is mandatory in the 'aws_ec2_rabbit' section of the configuration")

    if 'region_name' in config_data['aws_ec2_rabbit']:
        config_data['aws_ec2_rabbit']['region'] = config_data['aws_ec2_rabbit'].pop('region_name')

    if 'region' not in config_data['aws_ec2_rabbit']:
        raise Exception('"region" is mandatory under the "aws_ec2_rabbit" or "aws" section of the configuration')
    elif 'region' not in config_data['aws']:
        config_data['aws']['region'] = config_data['aws_ec2_rabbit']['region']

    if 'rabbitmq' in config_data and 'amqp_url' in config_data['rabbitmq']:
        config_data['aws_ec2_rabbit']['amqp_url'] = config_data['rabbitmq']['amqp_url']
