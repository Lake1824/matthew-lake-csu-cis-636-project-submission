import pytest
from opensearchpy import (
    OpenSearch,
    Document,
    Q,
    TransportError,
)

from tests.person import Person


def test_updating_a_document_with_dsl_033(
    test_os_client: OpenSearch,
    test_index_name: str,
    test_index_settings: dict,
    test_index_mapping: dict,
):
    # Setup
    create_index_body: dict = {
        "settings": test_index_settings,
        "mappings": test_index_mapping,
    }
    test_os_client.indices.create(index=test_index_name, body=create_index_body)
    document: Document = Person(id=1, name="Matthew", age=25)
    test_os_client.index(
        index=test_index_name, id=str(document.id), body=document.to_dict()
    )
    test_os_client.indices.refresh(index=test_index_name)

    # Exec
    updated_age: int = 26
    updated_document: Document = Person(id=document.id, name="Matthew", age=updated_age)
    test_os_client.update(
        index=test_index_name, id=document.id, body={"doc": updated_document.to_dict()}
    )
    test_os_client.indices.refresh(index=test_index_name)

    search_result = test_os_client.search(
        index=test_index_name,
        body={
            "query": Q(
                "term",
                _id=document.id,
            ).to_dict()
        },
    )

    # Assert
    assert len(search_result["hits"]["hits"]) == 1
    assert search_result["hits"]["hits"][0]["_source"]["age"] == updated_age


def test_not_updating_a_document_with_incorrect_id_034(
    test_os_client: OpenSearch,
    test_index_name: str,
    test_index_settings: dict,
    test_index_mapping: dict,
):
    # Setup
    create_index_body: dict = {
        "settings": test_index_settings,
        "mappings": test_index_mapping,
    }
    test_os_client.indices.create(index=test_index_name, body=create_index_body)
    document: Document = Person(id=1, name="Matthew", age=25)
    test_os_client.index(
        index=test_index_name, id=str(document.id), body=document.to_dict()
    )
    test_os_client.indices.refresh(index=test_index_name)

    # Exec
    updated_age: int = 26
    updated_document: Document = Person(id=document.id, name="Matthew", age=updated_age)
    test_os_client.update(
        index=test_index_name, id=document.id, body={"doc": updated_document.to_dict()}
    )

    # Exec/Assert
    with pytest.raises(TransportError):
        test_os_client.update(
            index=test_index_name, id=2, body={"doc": updated_document.to_dict()}
        )


def test_not_updating_a_document_due_to_non_existent_cluster_035(
    invalid_test_os_client: OpenSearch,
    test_index_name: str,
) -> None:
    # Exec/Assert
    with pytest.raises(TransportError):
        updated_document: Document = Person(id=1, name="Matthew", age=26)
        invalid_test_os_client.update(
            index=test_index_name, id=2, body={"doc": updated_document.to_dict()}
        )
