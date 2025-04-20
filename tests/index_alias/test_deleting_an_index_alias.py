from typing import Final

import pytest
from opensearchpy import OpenSearch, TransportError

ALIAS_TO_BE_DELETED: Final[str] = "test_index_alias"


def test_successfully_deleting_an_index_alias_023(
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
    test_os_client.indices.put_alias(index=test_index_name, name=ALIAS_TO_BE_DELETED)

    # Exec
    test_os_client.indices.delete_alias(index=test_index_name, name=ALIAS_TO_BE_DELETED)

    # Assert
    assert ALIAS_TO_BE_DELETED not in list(
        test_os_client.indices.get_alias(index=test_index_name)[test_index_name][
            "aliases"
        ].keys()
    )


def test_unsuccessfully_deleting_an_index_alias_for_an_non_existing_index_024(
    test_os_client: OpenSearch,
    test_index_name: str,
) -> None:
    # Exec/Assert
    with pytest.raises(TransportError):
        test_os_client.indices.delete_alias(
            index=test_index_name, name=ALIAS_TO_BE_DELETED
        )
