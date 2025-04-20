import pytest
from opensearchpy import (
    OpenSearch,
    Document,
    Q,
    helpers,
)
from opensearchpy.helpers import BulkIndexError
from opensearchpy.exceptions import ConnectionError

from tests.person import Person


def test_creating_and_bulk_indexing_documents_with_dsl_028(
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
    document_one: Document = Person(id=1, name="Matthew", age=25)
    document_two: Document = Person(id=2, name="Eric", age=22)
    bulk_index_action = [
        {
            "_op_type": "index",
            "_index": test_index_name,
            "_id": document_one.id,
            "_source": document_one.to_dict(),
        },
        {
            "_op_type": "index",
            "_index": test_index_name,
            "_id": document_two.id,
            "_source": document_two.to_dict(),
        },
    ]
    helpers.bulk(
        test_os_client,
        bulk_index_action,
    )
    test_os_client.indices.refresh(index=test_index_name)

    doc_in_index = [
        hit["_source"]
        for hit in test_os_client.search(
            index=test_index_name, body={"query": Q("match_all").to_dict()}
        )["hits"]["hits"]
    ]

    # Assert
    assert len(doc_in_index) == 2


def test_not_creating_and_indexing_a_document_with_misspelled_attribute_name_029(
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
    document_one: dict = {"id": 1, "names": "Alice", "age": 30}
    document_two: dict = {"id": 2, "names": "Bob", "age": 31}
    bulk_index_action = [
        {
            "_op_type": "index",
            "_index": test_index_name,
            "_id": document_one["id"],
            "_source": document_one,
        },
        {
            "_op_type": "index",
            "_index": test_index_name,
            "_id": document_two["id"],
            "_source": document_two,
        },
    ]

    # Assert
    with pytest.raises(BulkIndexError):
        helpers.bulk(
            test_os_client,
            bulk_index_action,
        )


def test_not_creating_and_indexing_a_document_due_to_non_existent_cluster_030(
    invalid_test_os_client: OpenSearch,
    test_index_name: str,
) -> None:
    # Exec/Assert
    with pytest.raises(ConnectionError):
        document_one: Document = Person(id=1, name="Matthew", age=25)
        document_two: Document = Person(id=2, name="Eric", age=22)
        bulk_index_action = [
            {
                "_op_type": "index",
                "_index": test_index_name,
                "_id": document_one.id,
                "_source": document_one.to_dict(),
            },
            {
                "_op_type": "index",
                "_index": test_index_name,
                "_id": document_two.id,
                "_source": document_two.to_dict(),
            },
        ]
        helpers.bulk(
            invalid_test_os_client,
            bulk_index_action,
        )
