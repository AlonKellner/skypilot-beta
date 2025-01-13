"""Test skypilot_config"""
import copy
import os
import pathlib
import textwrap
from unittest import mock

import pytest

import sky
from sky import skypilot_config
import sky.exceptions
from sky.server.requests import payloads
from sky.skylet import constants
from sky.utils import common_utils
from sky.utils import config_utils
from sky.utils import kubernetes_enums

DISK_ENCRYPTED = True
VPC_NAME = 'vpc-12345678'
PROXY_COMMAND = 'ssh -W %h:%p -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no'
NODEPORT_MODE_NAME = kubernetes_enums.KubernetesNetworkingMode.NODEPORT.value
PORT_FORWARD_MODE_NAME = kubernetes_enums.KubernetesNetworkingMode.PORTFORWARD.value
RUN_DURATION = 30
RUN_DURATION_OVERRIDE = 10
PROVISION_TIMEOUT = 600


def _check_empty_config() -> None:
    """Check that the config is empty."""
    assert not skypilot_config.loaded(), (skypilot_config._dict,
                                          skypilot_config._loaded_config_path)
    assert skypilot_config.get_nested(
        ('aws', 'ssh_proxy_command'), None) is None
    assert skypilot_config.get_nested(('aws', 'ssh_proxy_command'),
                                      'default') == 'default'


def _create_config_file(config_file_path: pathlib.Path) -> None:
    config_file_path.write_text(
        textwrap.dedent(f"""\
            aws:
                vpc_name: {VPC_NAME}
                use_internal_ips: true
                ssh_proxy_command: {PROXY_COMMAND}
                disk_encrypted: {DISK_ENCRYPTED}

            gcp:
                vpc_name: {VPC_NAME}
                use_internal_ips: true
                managed_instance_group:
                    run_duration: {RUN_DURATION}
                    provision_timeout: {PROVISION_TIMEOUT}

            kubernetes:
                networking: {NODEPORT_MODE_NAME}
                pod_config:
                    spec:
                        metadata:
                            annotations:
                                my_annotation: my_value
                        runtimeClassName: nvidia    # Custom runtimeClassName for GPU pods.
                        imagePullSecrets:
                            - name: my-secret     # Pull images from a private registry using a secret

            """))


def _create_task_yaml_file(task_file_path: pathlib.Path) -> None:
    task_file_path.write_text(
        textwrap.dedent(f"""\
        experimental:
            config_overrides:
                docker:
                    run_options:
                        - -v /tmp:/tmp
                kubernetes:
                    pod_config:
                        metadata:
                            labels:
                                test-key: test-value
                            annotations:
                                abc: def
                        spec:
                            imagePullSecrets:
                                - name: my-secret-2
                    provision_timeout: 100
                gcp:
                    managed_instance_group:
                        run_duration: {RUN_DURATION_OVERRIDE}
                nvidia_gpus:
                    disable_ecc: true
        resources:
            image_id: docker:ubuntu:latest

        setup: echo 'Setting up...'
        run: echo 'Running...'
        """))


def _create_invalid_config_yaml_file(task_file_path: pathlib.Path) -> None:
    task_file_path.write_text(
        textwrap.dedent("""\
        experimental:
            config_overrides:
                kubernetes:
                    pod_config:
                        metadata:
                            labels:
                                test-key: test-value
                            annotations:
                                abc: def
                        spec:
                            containers:
                                - name:
                            imagePullSecrets:
                                - name: my-secret-2

        setup: echo 'Setting up...'
        run: echo 'Running...'
        """))


def test_nested_config(monkeypatch) -> None:
    """Test that the nested config works."""
    config = skypilot_config.Config()
    config.set_nested(('aws', 'ssh_proxy_command'), 'value')
    assert config == {'aws': {'ssh_proxy_command': 'value'}}

    assert config.get_nested(('admin_policy',), 'default') == 'default'
    config.set_nested(('aws', 'use_internal_ips'), True)
    assert config == {
        'aws': {
            'ssh_proxy_command': 'value',
            'use_internal_ips': True
        }
    }


def test_no_config(monkeypatch) -> None:
    """Test that the config is not loaded if the config file does not exist."""
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', '/tmp/does_not_exist')
    skypilot_config._reload_config()
    _check_empty_config()


def test_empty_config(monkeypatch, tmp_path) -> None:
    """Test that the config is not loaded if the config file is empty."""
    with open(tmp_path / 'empty.yaml', 'w', encoding='utf-8') as f:
        f.write('')
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', tmp_path / 'empty.yaml')
    skypilot_config._reload_config()
    _check_empty_config()


def test_valid_null_proxy_config(monkeypatch, tmp_path) -> None:
    """Test that the config is not loaded if the config file is empty."""
    with open(tmp_path / 'valid.yaml', 'w', encoding='utf-8') as f:
        f.write(f"""\
        aws:
            labels:
                mytag: myvalue
            ssh_proxy_command:
                eu-west-1: null
                us-east-1: null
            use_internal_ips: true
            vpc_name: abc

        spot:
            controller:
                resources:
                    disk_size: 256
        """)
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', tmp_path / 'valid.yaml')
    skypilot_config._reload_config()
    proxy_config = skypilot_config.get_nested(
        ('aws', 'ssh_proxy_command', 'eu-west-1'), 'default')
    assert proxy_config is None, proxy_config


def test_invalid_field_config(monkeypatch, tmp_path) -> None:
    """Test that the config is not loaded if the config file contains unknown field."""
    config_path = tmp_path / 'invalid.yaml'
    config_path.open('w', encoding='utf-8').write(
        textwrap.dedent(f"""\
        aws:
            vpc_name: {VPC_NAME}
            not_a_field: 123
        """))
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH',
                        tmp_path / 'invalid.yaml')
    with pytest.raises(ValueError) as e:
        skypilot_config._reload_config()
    assert 'Invalid config YAML' in e.value.args[0]


def test_invalid_indent_config(monkeypatch, tmp_path) -> None:
    """Test that the config is not loaded if the config file is incorrectly indented."""
    config_path = tmp_path / 'invalid.yaml'
    config_path.open('w', encoding='utf-8').write(
        textwrap.dedent(f"""\
        spot:
            controller:
                resources:
                cloud: gcp
                instance_type: n2-standard-4
                cpus: 4
                disk_size: 50
        """))
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH',
                        tmp_path / 'invalid.yaml')
    with pytest.raises(ValueError) as e:
        skypilot_config._reload_config()
    assert 'Invalid config YAML' in e.value.args[0]


def test_invalid_enum_config(monkeypatch, tmp_path) -> None:
    """Test that the config is not loaded if the config file contains an invalid enum value."""
    config_path = tmp_path / 'invalid.yaml'
    config_path.open('w', encoding='utf-8').write(
        textwrap.dedent(f"""\
        spot:
            controller:
                resources:
                    cloud: notacloud
        """))
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH',
                        tmp_path / 'invalid.yaml')
    with pytest.raises(ValueError) as e:
        skypilot_config._reload_config()
    assert 'Invalid config YAML' in e.value.args[0]


def test_valid_num_items_config(monkeypatch, tmp_path) -> None:
    """Test that the config is not loaded if the config file contains an invalid number of array items."""
    config_path = tmp_path / 'valid.yaml'
    config_path.open('w', encoding='utf-8').write(
        textwrap.dedent(f"""\
        gcp:
            specific_reservations:
                - projects/my-project/reservations/my-reservation
                - projects/my-project/reservations/my-reservation2
        """))
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', tmp_path / 'valid.yaml')
    skypilot_config._reload_config()


def test_config_get_set_nested(monkeypatch, tmp_path) -> None:
    """Test that set_nested(), get_nested() works."""

    # Load from a config file
    config_path = tmp_path / 'config.yaml'
    _create_config_file(config_path)
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', config_path)
    skypilot_config._reload_config()
    # Check that the config is loaded with the expected values
    assert skypilot_config.loaded()
    assert skypilot_config.get_nested(('aws', 'vpc_name'), None) == VPC_NAME
    assert skypilot_config.get_nested(('aws', 'disk_encrypted'), None)
    assert skypilot_config.get_nested(('aws', 'use_internal_ips'), None)
    assert skypilot_config.get_nested(('aws', 'ssh_proxy_command'),
                                      None) == PROXY_COMMAND
    assert skypilot_config.get_nested(('gcp', 'vpc_name'), None) == VPC_NAME
    assert skypilot_config.get_nested(('kubernetes', 'networking'),
                                      None) == NODEPORT_MODE_NAME
    # Check set_nested() will copy the config dict and return a new dict
    new_config = skypilot_config.set_nested(('aws', 'ssh_proxy_command'),
                                            'new_value')
    assert new_config['aws']['ssh_proxy_command'] == 'new_value'
    assert skypilot_config.get_nested(('aws', 'ssh_proxy_command'),
                                      None) == PROXY_COMMAND
    new_config = skypilot_config.set_nested(('kubernetes', 'networking'),
                                            PORT_FORWARD_MODE_NAME)
    assert skypilot_config.get_nested(('kubernetes', 'networking'),
                                      None) == NODEPORT_MODE_NAME
    # Check that dumping the config to a file with the new None can be reloaded
    new_config2 = skypilot_config.set_nested(('aws', 'ssh_proxy_command'), None)
    new_config_path = tmp_path / 'new_config.yaml'
    common_utils.dump_yaml(new_config_path, new_config2)
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', new_config_path)
    skypilot_config._reload_config()
    assert skypilot_config.get_nested(('aws', 'vpc_name'), None) == VPC_NAME
    assert skypilot_config.get_nested(('aws', 'use_internal_ips'), None)
    assert skypilot_config.get_nested(
        ('aws', 'ssh_proxy_command'), None) is None
    assert skypilot_config.get_nested(('gcp', 'vpc_name'), None) == VPC_NAME
    assert skypilot_config.get_nested(('gcp', 'use_internal_ips'), None)

    # Check config with only partial keys still works
    new_config3 = copy.copy(new_config2)
    del new_config3['aws']['ssh_proxy_command']
    del new_config3['aws']['use_internal_ips']
    new_config_path = tmp_path / 'new_config3.yaml'
    common_utils.dump_yaml(new_config_path, new_config3)
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', new_config_path)
    skypilot_config._reload_config()
    assert skypilot_config.get_nested(('aws', 'vpc_name'), None) == VPC_NAME
    assert skypilot_config.get_nested(('aws', 'use_internal_ips'), None) is None
    assert skypilot_config.get_nested(
        ('aws', 'ssh_proxy_command'), None) is None
    # set_nested() should still work
    new_config4 = skypilot_config.set_nested(('aws', 'ssh_proxy_command'),
                                             'new_value')
    assert new_config4['aws']['ssh_proxy_command'] == 'new_value'
    assert skypilot_config.get_nested(
        ('aws', 'ssh_proxy_command'), None) is None


def test_config_with_env(monkeypatch, tmp_path) -> None:
    """Test that the config is loaded with environment variables."""
    config_path = tmp_path / 'config.yaml'
    _create_config_file(config_path)
    monkeypatch.setenv(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, config_path)
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH',
                        tmp_path / 'does_not_exist')
    skypilot_config._reload_config()
    assert skypilot_config.loaded()
    assert skypilot_config.get_nested(('aws', 'vpc_name'), None) == VPC_NAME
    assert skypilot_config.get_nested(('aws', 'use_internal_ips'), None)
    assert skypilot_config.get_nested(('aws', 'ssh_proxy_command'),
                                      None) == PROXY_COMMAND
    assert skypilot_config.get_nested(('gcp', 'vpc_name'), None) == VPC_NAME
    assert skypilot_config.get_nested(('gcp', 'use_internal_ips'), None)


def test_k8s_config_with_override(monkeypatch, tmp_path,
                                  enable_all_clouds) -> None:
    config_path = tmp_path / 'config.yaml'
    _create_config_file(config_path)
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', config_path)

    skypilot_config._reload_config()
    task_path = tmp_path / 'task.yaml'
    _create_task_yaml_file(task_path)
    task = sky.Task.from_yaml(task_path)

    # Test Kubernetes overrides
    # Get cluster YAML
    cluster_name = 'test-kubernetes-config-with-override'
    task.set_resources_override({'cloud': sky.Kubernetes()})
    sky.launch(task, cluster_name=cluster_name, dryrun=True)
    cluster_yaml = pathlib.Path(
        f'~/.sky/generated/{cluster_name}.yml.tmp').expanduser().rename(
            tmp_path / (cluster_name + '.yml'))

    # Load the cluster YAML
    cluster_config = common_utils.read_yaml(cluster_yaml)
    head_node_type = cluster_config['head_node_type']
    cluster_pod_config = cluster_config['available_node_types'][head_node_type][
        'node_config']
    assert cluster_pod_config['metadata']['labels']['test-key'] == 'test-value'
    assert cluster_pod_config['metadata']['labels']['parent'] == 'skypilot'
    assert cluster_pod_config['metadata']['annotations']['abc'] == 'def'
    assert len(cluster_pod_config['spec']
               ['imagePullSecrets']) == 1 and cluster_pod_config['spec'][
                   'imagePullSecrets'][0]['name'] == 'my-secret-2'
    assert cluster_pod_config['spec']['runtimeClassName'] == 'nvidia'


def test_k8s_config_with_invalid_config(monkeypatch, tmp_path,
                                        enable_all_clouds) -> None:
    config_path = tmp_path / 'config.yaml'
    _create_config_file(config_path)
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', config_path)

    _reload_config()
    task_path = tmp_path / 'task.yaml'
    _create_invalid_config_yaml_file(task_path)
    task = sky.Task.from_yaml(task_path)

    # Test Kubernetes pod_config invalid
    cluster_name = 'test_k8s_config_with_invalid_config'
    task.set_resources_override({'cloud': sky.Kubernetes()})
    exception_occurred = False
    try:
        sky.launch(task, cluster_name=cluster_name, dryrun=True)
    except sky.exceptions.ResourcesUnavailableError:
        exception_occurred = True
    assert exception_occurred


def test_gcp_config_with_override(monkeypatch, tmp_path,
                                  enable_all_clouds) -> None:
    config_path = tmp_path / 'config.yaml'
    _create_config_file(config_path)
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', config_path)

    skypilot_config._reload_config()
    task_path = tmp_path / 'task.yaml'
    _create_task_yaml_file(task_path)
    task = sky.Task.from_yaml(task_path)

    # Test GCP overrides
    cluster_name = 'test-gcp-config-with-override'
    task.set_resources_override({'cloud': sky.GCP(), 'accelerators': 'L4'})
    sky.launch(task, cluster_name=cluster_name, dryrun=True)
    cluster_yaml = pathlib.Path(
        f'~/.sky/generated/{cluster_name}.yml.tmp').expanduser().rename(
            tmp_path / (cluster_name + '.yml'))

    # Load the cluster YAML
    cluster_config = common_utils.read_yaml(cluster_yaml)
    assert cluster_config['provider']['vpc_name'] == VPC_NAME
    assert '-v /tmp:/tmp' in cluster_config['docker'][
        'run_options'], cluster_config
    assert constants.DISABLE_GPU_ECC_COMMAND in cluster_config[
        'setup_commands'][0]
    head_node_type = cluster_config['head_node_type']
    cluster_node_config = cluster_config['available_node_types'][
        head_node_type]['node_config']
    assert cluster_node_config['managed-instance-group'][
        'run_duration'] == RUN_DURATION_OVERRIDE
    assert cluster_node_config['managed-instance-group'][
        'provision_timeout'] == PROVISION_TIMEOUT


def test_config_with_invalid_override(monkeypatch, tmp_path,
                                      enable_all_clouds) -> None:
    config_path = tmp_path / 'config.yaml'
    _create_config_file(config_path)
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', config_path)

    skypilot_config._reload_config()

    task_config_yaml = textwrap.dedent(f"""\
        experimental:
            config_overrides:
                gcp:
                    vpc_name: abc
        resources:
            image_id: docker:ubuntu:latest

        setup: echo 'Setting up...'
        run: echo 'Running...'
        """)

    with pytest.raises(ValueError, match='Found unsupported') as e:
        task_path = tmp_path / 'task.yaml'
        task_path.write_text(task_config_yaml)
        sky.Task.from_yaml(task_path)


@mock.patch('sky.skypilot_config.to_dict',
            return_value={
                'api_server': 'http://example.com',
                'aws': {
                    'vpc_name': 'test-vpc',
                    'security_group': 'test-sg'
                },
                'gcp': {
                    'project_id': 'test-project'
                }
            })
@mock.patch('sky.skylet.constants.SKIPPED_CLIENT_OVERRIDE_KEYS',
            [('aws', 'security_group')])
@mock.patch('sky.skypilot_config.loaded_config_path',
            return_value='/path/to/config.yaml')
def test_get_override_skypilot_config_from_client(mock_to_dict, mock_logger):
    with mock.patch('sky.server.requests.payloads.logger') as mock_logger:
        # Call the function
        result = payloads.get_override_skypilot_config_from_client()

        # Verify api_server was removed
        assert 'api_server' not in result

        # Verify disallowed key was removed
        assert 'security_group' not in result['aws']

        # Verify allowed keys remain
        assert result['aws']['vpc_name'] == 'test-vpc'
        assert result['gcp']['project_id'] == 'test-project'

        # Verify warning was logged for removed disallowed key
        mock_logger.warning.assert_called_once_with(
            'Key (\'aws\', \'security_group\') is specified in the client '
            'SkyPilot config at \'/path/to/config.yaml\'. This will be '
            'ignored. If you want to specify it, please modify it on server '
            'side or contact your administrator.')


def test_override_skypilot_config(monkeypatch, tmp_path):
    """Test that override_skypilot_config properly restores config and cleans up."""
    os.environ.pop(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, None)
    # Create original config file
    config_path = tmp_path / 'config.yaml'
    _create_config_file(config_path)
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', config_path)
    skypilot_config._reload_config()

    # Store original values
    original_vpc = skypilot_config.get_nested(('aws', 'vpc_name'), None)
    original_proxy = skypilot_config.get_nested(('aws', 'ssh_proxy_command'),
                                                None)

    # Create override config
    override_configs = {
        'aws': {
            'vpc_name': 'override-vpc',
            'ssh_proxy_command': 'override-command'
        }
    }

    # Use the context manager
    with skypilot_config.override_skypilot_config(override_configs):
        # Check values are overridden
        assert skypilot_config.get_nested(('aws', 'vpc_name'),
                                          None) == 'override-vpc'
        assert skypilot_config.get_nested(('aws', 'ssh_proxy_command'),
                                          None) == 'override-command'

        # Get the temp file path
        temp_config_path = os.environ.get(
            skypilot_config.ENV_VAR_SKYPILOT_CONFIG)
        assert temp_config_path is not None
        assert os.path.exists(temp_config_path)

    # Check values are restored
    assert skypilot_config.get_nested(('aws', 'vpc_name'), None) == original_vpc
    assert skypilot_config.get_nested(('aws', 'ssh_proxy_command'),
                                      None) == original_proxy

    # Check temp file is cleaned up
    assert not os.path.exists(temp_config_path)
    env_config_path = os.environ.get(skypilot_config.ENV_VAR_SKYPILOT_CONFIG)
    assert env_config_path == str(config_path), (
        f'{env_config_path} is not cleaned up')

    # Test with None override_configs
    with skypilot_config.override_skypilot_config(None):
        assert skypilot_config.get_nested(('aws', 'vpc_name'),
                                          None) == original_vpc
        assert skypilot_config.get_nested(('aws', 'ssh_proxy_command'),
                                          None) == original_proxy


def test_override_skypilot_config_without_original_config(
        monkeypatch, tmp_path):
    """Test that override_skypilot_config works with empty original config."""
    os.environ.pop(skypilot_config.ENV_VAR_SKYPILOT_CONFIG, None)
    # Create original config file
    config_path = tmp_path / 'non_existent.yaml'
    monkeypatch.setattr(skypilot_config, 'CONFIG_PATH', config_path)
    skypilot_config._reload_config()
    assert not skypilot_config._dict
    assert skypilot_config._loaded_config_path is None
    assert skypilot_config.get_nested(('aws', 'vpc_name'), None) is None

    # Test empty override_configs
    with skypilot_config.override_skypilot_config(None):
        assert skypilot_config.get_nested(('aws', 'vpc_name'), None) is None
        assert skypilot_config.get_nested(
            ('aws', 'ssh_proxy_command'), None) is None

        assert os.environ.get(skypilot_config.ENV_VAR_SKYPILOT_CONFIG) is None

    # Test with override_configs
    # Create override config
    override_configs = {
        'aws': {
            'vpc_name': 'override-vpc',
            'ssh_proxy_command': 'override-command'
        }
    }

    with skypilot_config.override_skypilot_config(override_configs):
        assert skypilot_config.get_nested(('aws', 'vpc_name'),
                                          None) == 'override-vpc'
        assert skypilot_config.get_nested(('aws', 'ssh_proxy_command'),
                                          None) == 'override-command'

        # Get the temp file path
        temp_config_path = os.environ.get(
            skypilot_config.ENV_VAR_SKYPILOT_CONFIG)
        assert temp_config_path is not None
        assert os.path.exists(temp_config_path)

    # Check values are restored
    assert skypilot_config.get_nested(('aws', 'vpc_name'), None) is None
    assert skypilot_config.get_nested(
        ('aws', 'ssh_proxy_command'), None) is None
    assert os.environ.get(skypilot_config.ENV_VAR_SKYPILOT_CONFIG) is None
    assert skypilot_config._loaded_config_path is None
    assert not skypilot_config._dict
