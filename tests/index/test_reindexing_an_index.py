import pytest
from opensearchpy import OpenSearch, TransportError, Q, UnknownDslObject


def test_successfully_reindexing_an_index_with_changing_the_index_name_015(
    test_os_client: OpenSearch,
    test_index_name: str,
    test_index_settings: dict,
    test_index_mapping: dict,
    test_index_documents: list[dict],
) -> None:
    # Setup
    create_index_body: dict = {
        "settings": test_index_settings,
        "mappings": test_index_mapping,
    }
    test_os_client.indices.create(index=test_index_name, body=create_index_body)

    for doc in test_index_documents:
        test_os_client.index(index=test_index_name, id=doc["id"], body=doc)

    test_os_client.indices.refresh(index=test_index_name)

    new_index_name: str = "new_index"
    test_os_client.indices.create(index=new_index_name, body=create_index_body)

    # Exec
    reindex_body = {
        "source": {"index": test_index_name},
        "dest": {"index": new_index_name},
    }
    test_os_client.reindex(body=reindex_body, wait_for_completion=True)
    test_os_client.indices.refresh(index=new_index_name)

    docs_in_new_index = [
        hit["_source"]
        for hit in test_os_client.search(
            index=new_index_name, body={"query": Q("match_all").to_dict()}
        )["hits"]["hits"]
    ]

    # Assert
    assert len(docs_in_new_index) == len(test_index_documents)


def test_successfully_reindexing_an_index_with_only_moving_documents_that_have_age_greater_than_35_016(
    test_os_client: OpenSearch,
    test_index_name: str,
    test_index_settings: dict,
    test_index_mapping: dict,
    test_index_documents: list[dict],
) -> None:
    # Setup
    create_index_body: dict = {
        "settings": test_index_settings,
        "mappings": test_index_mapping,
    }
    test_os_client.indices.create(index=test_index_name, body=create_index_body)

    for doc in test_index_documents:
        test_os_client.index(index=test_index_name, id=doc["id"], body=doc)

    test_os_client.indices.refresh(index=test_index_name)

    new_index_name: str = "age_greater_than_35_index"
    test_os_client.indices.create(index=new_index_name, body=create_index_body)

    # Exec
    reindex_body = {
        "source": {
            "index": test_index_name,
            "query": Q("range", age={"gt": 35}).to_dict(),
        },
        "dest": {"index": new_index_name},
    }
    test_os_client.reindex(body=reindex_body, wait_for_completion=True)
    test_os_client.indices.refresh(index=new_index_name)

    docs_in_new_index = [
        hit["_source"]
        for hit in test_os_client.search(
            index=new_index_name, body={"query": Q("match_all").to_dict()}
        )["hits"]["hits"]
    ]

    # Assert
    assert len(docs_in_new_index) == 1


def test_unsuccessfully_reindexing_an_non_existing_index_017(
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
    new_index_name: str = "empty_index"
    test_os_client.indices.create(index=new_index_name, body=create_index_body)

    # Exec/Assert
    with pytest.raises(TransportError):
        reindex_body = {
            "source": {"index": test_index_name},
            "dest": {"index": new_index_name},
        }
        test_os_client.reindex(body=reindex_body, wait_for_completion=True)


def test_unsuccessfully_reindexing_an_index_with_invalid_query_018(
    test_os_client: OpenSearch,
    test_index_name: str,
    test_index_settings: dict,
    test_index_mapping: dict,
    test_index_documents: list[dict],
) -> None:
    # Setup
    create_index_body: dict = {
        "settings": test_index_settings,
        "mappings": test_index_mapping,
    }
    test_os_client.indices.create(index=test_index_name, body=create_index_body)

    for doc in test_index_documents:
        test_os_client.index(index=test_index_name, id=doc["id"], body=doc)

    test_os_client.indices.refresh(index=test_index_name)

    new_index_name: str = "test_invalid_query_index"
    test_os_client.indices.create(index=new_index_name, body=create_index_body)

    # Exec/Assert
    with pytest.raises(UnknownDslObject):
        reindex_body = {
            "source": {
                "index": test_index_name,
                "query": Q("blah", age={"gt": 35}).to_dict(),
            },
            "dest": {"index": new_index_name},
        }
        test_os_client.reindex(body=reindex_body, wait_for_completion=True)
