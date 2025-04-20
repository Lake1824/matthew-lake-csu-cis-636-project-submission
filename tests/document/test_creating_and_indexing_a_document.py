import pytest
from opensearchpy import (
    OpenSearch,
    Document,
    Q,
    RequestError,
    TransportError,
)

from tests.person import Person


def test_creating_and_indexing_a_document_with_dsl_025(
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

    # Exec
    document: Document = Person(id=1, name="Matthew", age=25)
    test_os_client.index(index=test_index_name, id=document.id, body=document.to_dict())

    test_os_client.indices.refresh(index=test_index_name)

    doc_in_index = [
        hit["_source"]
        for hit in test_os_client.search(
            index=test_index_name, body={"query": Q("match_all").to_dict()}
        )["hits"]["hits"]
    ]

    # Assert
    assert len(doc_in_index) == 1


def test_not_creating_and_indexing_a_document_with_misspelled_attribute_name_026(
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
    document: dict = {"id": 1, "names": "Alice", "age": 30}

    # Exec/Assert
    with pytest.raises(RequestError):
        test_os_client.index(index=test_index_name, id=document["id"], body=document)


def test_not_creating_and_indexing_a_document_due_to_non_existent_cluster_027(
    invalid_test_os_client: OpenSearch,
    test_index_name: str,
) -> None:
    # Exec/Assert
    document: dict = {"id": 1, "names": "Alice", "age": 30}
    with pytest.raises(TransportError):
        invalid_test_os_client.index(
            index=test_index_name, id=document["id"], body=document
        )
