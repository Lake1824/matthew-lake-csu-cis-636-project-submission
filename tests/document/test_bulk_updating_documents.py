import pytest
from opensearchpy import (
    OpenSearch,
    Document,
    Q,
)
from opensearchpy.helpers import bulk, BulkIndexError
from opensearchpy.exceptions import ConnectionError

from tests.person import Person


def test_successfully_bulk_updating_documents_036(
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
    document_two: Document = Person(id=2, name="Eric", age=22)
    test_os_client.index(
        index=test_index_name, id=str(document_one.id), body=document_one.to_dict()
    )
    test_os_client.index(
        index=test_index_name, id=str(document_two.id), body=document_two.to_dict()
    )
    test_os_client.indices.refresh(index=test_index_name)

    # Exec
    bulk_updating_action = [
        {
            "_op_type": "update",
            "_index": test_index_name,
            "_id": doc.id,
            "script": {
                "source": "ctx._source.age += params.increment",
                "lang": "painless",
                "params": {"increment": 1},
            },
        }
        for doc in [document_one, document_two]
    ]
    bulk(test_os_client, bulk_updating_action)
    test_os_client.indices.refresh(index=test_index_name)

    bulk_updated_docs = [
        hit["_source"]
        for hit in test_os_client.search(
            index=test_index_name, body={"query": Q("match_all").to_dict()}
        )["hits"]["hits"]
    ]

    # Assert
    assert len(bulk_updated_docs) == 2
    assert bulk_updated_docs[0]["age"] == document_one.age + 1
    assert bulk_updated_docs[1]["age"] == document_two.age + 1


def test_not_bulk_updating_a_document_due_to_incorrect_script_037(
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
    document_two: Document = Person(id=2, name="Eric", age=22)
    test_os_client.index(
        index=test_index_name, id=str(document_one.id), body=document_one.to_dict()
    )
    test_os_client.index(
        index=test_index_name, id=str(document_two.id), body=document_two.to_dict()
    )
    test_os_client.indices.refresh(index=test_index_name)

    # Exec/Assert
    with pytest.raises(BulkIndexError):
        bulk_updating_action = [
            {
                "_op_type": "update",
                "_index": test_index_name,
                "_id": doc.id,
                "script": {
                    "source": "ctx._source.age += params.increment",
                    "lang": "painlessssssssssss",
                    "params": {"increment": 1},
                },
            }
            for doc in [document_one, document_two]
        ]
        bulk(test_os_client, bulk_updating_action)


def test_not_bulk_updating_a_document_due_to_missing_index_038(
    invalid_test_os_client: OpenSearch,
    test_index_name: str,
):
    # Setup
    document_one: Document = Person(id=1, name="Matthew", age=25)
    document_two: Document = Person(id=2, name="Eric", age=22)

    # Exec/Assert
    with pytest.raises(ConnectionError):
        bulk_updating_action = [
            {
                "_op_type": "update",
                "_index": test_index_name,
                "_id": doc.id,
                "script": {
                    "source": "ctx._source.age += params.increment",
                    "lang": "painless",
                    "params": {"increment": 1},
                },
            }
            for doc in [document_one, document_two]
        ]
        bulk(invalid_test_os_client, bulk_updating_action)
