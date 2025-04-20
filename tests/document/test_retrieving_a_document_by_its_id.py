import pytest
from opensearchpy import OpenSearch, Q, TransportError


def test_successfully_retrieving_a_document_by_its_id_031(
    test_os_client: OpenSearch,
    test_index_name: str,
    test_index_settings: dict,
    test_index_mapping: dict,
    test_index_documents: list[dict],
):
    # Setup
    create_index_body: dict = {
        "settings": test_index_settings,
        "mappings": test_index_mapping,
    }
    test_os_client.indices.create(index=test_index_name, body=create_index_body)

    for doc in test_index_documents:
        test_os_client.index(index=test_index_name, id=doc["id"], body=doc)

    test_os_client.indices.refresh(index=test_index_name)

    # Exec
    search_result = test_os_client.search(
        index=test_index_name,
        body={
            "query": Q(
                "term",
                _id=1,
            ).to_dict()
        },
    )

    # Assert
    assert len(search_result["hits"]["hits"]) == 1


def test_not_retrieving_a_document_by_its_id_due_to_non_existent_cluster_032(
    invalid_test_os_client: OpenSearch,
    test_index_name: str,
) -> None:
    # Exec/Assert
    with pytest.raises(TransportError):
        invalid_test_os_client.search(
            index=test_index_name,
            body={
                "query": Q(
                    "term",
                    _id=1,
                ).to_dict()
            },
        )
