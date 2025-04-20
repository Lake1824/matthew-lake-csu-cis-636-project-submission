import pytest
from opensearchpy import (
    OpenSearch,
    Document,
    Q,
    TransportError,
)

from tests.person import Person


def test_successfully_deleting_a_document_039(
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
    document_one: Document = Person(id=1, name="Matthew", age=25)
    document_two: Document = Person(id=2, name="John", age=25)
    test_os_client.index(
        index=test_index_name, id=document_one.id, body=document_one.to_dict()
    )
    test_os_client.index(
        index=test_index_name, id=document_two.id, body=document_two.to_dict()
    )
    test_os_client.indices.refresh(index=test_index_name)

    # Exec
    test_os_client.delete(index=test_index_name, id=document_one.id)
    test_os_client.indices.refresh(index=test_index_name)

    index_docs = [
        hit["_source"]
        for hit in test_os_client.search(
            index=test_index_name, body={"query": Q("match_all").to_dict()}
        )["hits"]["hits"]
    ]

    # Assert
    assert len(index_docs) == 1
    assert index_docs[0]["id"] == 2


def test_not_deleting_a_document_due_to_non_existent_cluster_040(
    invalid_test_os_client: OpenSearch,
    test_index_name: str,
) -> None:
    # Exec/Assert
    with pytest.raises(TransportError):
        invalid_test_os_client.delete(index=test_index_name, id=1)
