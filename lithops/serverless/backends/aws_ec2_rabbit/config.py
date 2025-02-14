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

DEFAULT_CONFIG_KEYS = {
    'worker_instance_type': 't2.micro',  # 't2.medium',
    'request_spot_instances': True,
    'max_workers': 100,
    'runtime_memory': 512,
    'worker_processes': 1,
    'runtime_timeout': 600,  # Default: 10 minutes
    'exec_mode': 'reuse',
    'docker_server': 'docker.io'
}

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
    
    if 'target_ami' not in config_data['aws_ec2_rabbit']:
        raise Exception('"target_ami" is mandatory under the "aws_ec2_rabbit" section of the configuration')
    else:
        config_data['aws_ec2_rabbit']['runtime'] = config_data['aws_ec2_rabbit']['target_ami']

    if 'region' not in config_data['aws_ec2_rabbit']:
        raise Exception('"region" is mandatory under the "aws_ec2_rabbit" or "aws" section of the configuration')
    elif 'region' not in config_data['aws']:
        config_data['aws']['region'] = config_data['aws_ec2_rabbit']['region']

    if 'rabbitmq' in config_data and 'amqp_url' in config_data['rabbitmq']:
        config_data['aws_ec2_rabbit']['amqp_url'] = config_data['rabbitmq']['amqp_url']
