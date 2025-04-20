from typing import Final

import pytest
from opensearchpy import OpenSearch, TransportError

CURRENT_ALIAS_NAME: Final[str] = "test_index_alias"
NEW_ALIAS_NAME: Final[str] = "new_test_index_alias"


def test_successfully_updating_an_index_alias_021(
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
    test_os_client.indices.put_alias(index=test_index_name, name=CURRENT_ALIAS_NAME)

    # Exec
    test_os_client.indices.put_alias(index=test_index_name, name=NEW_ALIAS_NAME)

    # Assert
    assert NEW_ALIAS_NAME in list(
        test_os_client.indices.get_alias(index=test_index_name)[test_index_name][
            "aliases"
        ].keys()
    )


def test_unsuccessfully_updating_an_index_alias_for_an_non_existing_index_022(
    test_os_client: OpenSearch,
    test_index_name: str,
) -> None:
    # Exec/Assert
    with pytest.raises(TransportError):
        test_os_client.indices.put_alias(index=test_index_name, name=NEW_ALIAS_NAME)
