import pytest
from opensearchpy import (
    OpenSearch,
    RequestError,
    TransportError,
)


def test_successfully_creating_an_index_003(
    test_os_client: OpenSearch,
    test_index_name: str,
    test_index_settings: dict,
    test_index_mapping: dict,
) -> None:
    # Setup
    create_index_body: dict = {
        "settings": test_index_settings,
        "mappings": test_index_mapping,
    }
    # Exec
    test_os_client.indices.create(index=test_index_name, body=create_index_body)

    actual_settings = test_os_client.indices.get_settings(index=test_index_name)[
        test_index_name
    ]["settings"]["index"]

    actual_mapping = test_os_client.indices.get_mapping(index=test_index_name)[
        test_index_name
    ]["mappings"]

    # Assert
    assert test_os_client.indices.exists(index=test_index_name)
    assert test_index_settings["index.number_of_shards"] == int(
        actual_settings["number_of_shards"]
    )
    assert test_index_settings["index.number_of_replicas"] == int(
        actual_settings["number_of_replicas"]
    )
    assert test_index_mapping == actual_mapping


def test_unsuccessfully_creating_an_index_due_to_incorrectly_formatted_mapping_004(
    test_os_client: OpenSearch,
    test_index_name: str,
    test_index_settings: dict,
    test_index_mapping: dict,
) -> None:
    # Setup
    create_index_body: dict = {
        "settings": test_index_settings,
        "mappingss": test_index_mapping,
    }

    # Exec/Assert
    with pytest.raises(RequestError):
        test_os_client.indices.create(index=test_index_name, body=create_index_body)


def test_unsuccessfully_creating_an_index_due_to_incorrect_mapping_type_005(
    test_os_client: OpenSearch,
    test_index_name: str,
    test_index_settings: dict,
) -> None:
    # Setup
    incorrect_mapping_type_mapping: dict = {
        "properties": {
            "name": {"type": "texts"},
            "age": {"type": "integer"},
        }
    }
    create_index_body: dict = {
        "settings": test_index_settings,
        "mappings": incorrect_mapping_type_mapping,
    }

    # Exec/Assert
    with pytest.raises(RequestError):
        test_os_client.indices.create(index=test_index_name, body=create_index_body)


def test_unsuccessfully_creating_an_index_due_to_incorrect_setting_006(
    test_os_client: OpenSearch,
    test_index_name: str,
    test_index_mapping: dict,
) -> None:
    # Setup
    incorrect_index_setting_index_settings: dict = {
        "index.number_of_shard": 1,
        "index.number_of_replicas": 2,
    }
    create_index_body: dict = {
        "settings": incorrect_index_setting_index_settings,
        "mappings": test_index_mapping,
    }

    # Exec/Assert
    with pytest.raises(RequestError):
        test_os_client.indices.create(index=test_index_name, body=create_index_body)


def test_unsuccessfully_creating_an_index_due_to_non_existent_cluster_007(
    invalid_test_os_client: OpenSearch,
    test_index_name: str,
) -> None:
    # Setup
    index_settings: dict = {
        "settings": {
            "index.number_of_shards": 2,
            "index.number_of_replicas": 2,
            "index.refresh_interval": "1s",
        }
    }

    # Exec/Assert
    with pytest.raises(TransportError):
        invalid_test_os_client.indices.put_settings(
            index=test_index_name, body=index_settings
        )
