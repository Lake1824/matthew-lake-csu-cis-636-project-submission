import pytest
from opensearchpy import (
    OpenSearch,
    TransportError,
)


def test_successfully_deleting_an_index_013(
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

    # Exec
    test_os_client.indices.delete(index=test_index_name)

    # Assert
    assert not test_os_client.indices.exists(index=test_index_name)


def test_unsuccessfully_modifying_an_index_due_to_non_existent_cluster_014(
    invalid_test_os_client: OpenSearch,
    test_index_name: str,
) -> None:
    # Exec/Assert
    with pytest.raises(TransportError):
        assert not invalid_test_os_client.indices.exists(index=test_index_name)
