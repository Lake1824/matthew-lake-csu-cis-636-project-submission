import pytest
from opensearchpy import (
    OpenSearch,
    RequestError,
    TransportError,
)


def test_successfully_modifying_an_index_settings_by_adding_a_setting_008(
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
    test_os_client.indices.create(index=test_index_name, body=create_index_body)

    new_settings: dict = {
        "settings": {
            "index.refresh_interval": "1s",
        }
    }

    # Exec
    test_os_client.indices.put_settings(index=test_index_name, body=new_settings)

    actual_settings = test_os_client.indices.get_settings(index=test_index_name)[
        test_index_name
    ]["settings"]["index"]

    # Assert
    assert (
        new_settings["settings"]["index.refresh_interval"]
        == actual_settings["refresh_interval"]
    )


def test_successfully_modifying_an_index_mappings_by_adding_a_field_009(
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
    test_os_client.indices.create(index=test_index_name, body=create_index_body)

    new_mapping: dict = {
        "dynamic": "strict",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "text"},
            "age": {"type": "integer"},
            "height": {"type": "integer"},
            "gender": {"type": "text"},
            "house_location": {
                "type": "nested",
                "properties": {
                    "location": {"type": "geo_shape"},
                    "city": {"type": "text"},
                },
            },
        },
    }

    # Exec
    test_os_client.indices.put_mapping(index=test_index_name, body=new_mapping)

    actual_mapping = test_os_client.indices.get_mapping(index=test_index_name)[
        test_index_name
    ]["mappings"]

    # Assert
    assert actual_mapping == new_mapping


def test_unsuccessfully_modifying_an_index_due_to_incorrect_index_setting_010(
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
    test_os_client.indices.create(index=test_index_name, body=create_index_body)

    incorrect_setting_settings: dict = {
        "settings": {
            "index.number_of_shards": 1,
            "index.number_of_replicas": 2,
            "index.refresh_interva": "1s",
        }
    }

    # Exec/Assert
    with pytest.raises(RequestError):
        test_os_client.indices.put_settings(
            index=test_index_name, body=incorrect_setting_settings
        )


def test_unsuccessfully_modifying_an_index_due_to_incorrect_mapping_attribute_type_011(
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
    test_os_client.indices.create(index=test_index_name, body=create_index_body)

    incorrect_mapping_type_mapping: dict = {
        "properties": {
            "name": {"type": "texts"},
            "age": {"type": "integer"},
        }
    }

    # Exec/Assert
    with pytest.raises(RequestError):
        test_os_client.indices.put_mapping(
            index=test_index_name, body=incorrect_mapping_type_mapping
        )


def test_unsuccessfully_modifying_an_index_due_to_non_existent_cluster_012(
    invalid_test_os_client: OpenSearch,
    test_index_name: str,
) -> None:
    # Setup
    index_settings: dict = {
        "settings": {
            "index.number_of_shards": 1,
            "index.number_of_replicas": 2,
            "index.refresh_interval": "1s",
        }
    }

    # Exec/Assert
    with pytest.raises(TransportError):
        invalid_test_os_client.indices.put_settings(
            index=test_index_name, body=index_settings
        )
