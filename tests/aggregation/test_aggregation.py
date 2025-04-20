import pytest
from opensearchpy import (
    OpenSearch,
    Search,
    UnknownDslObject,
    TransportError,
)


def test_bucket_aggregation_049(
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
    search = Search(index=test_index_name)
    search.aggs.bucket(name="age_bucket", agg_type="terms", field="age")
    aggregation_result = search.using(test_os_client).execute()

    # Assert
    assert aggregation_result["hits"]["total"]["value"] == 5


def test_metric_aggregation_050(
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
    search = Search(index=test_index_name)
    search.aggs.metric(name="min_age", agg_type="min", field="age")
    aggregation_result = search.using(test_os_client).execute()

    # Assert
    assert aggregation_result.aggregations.min_age["value"] == 22.0


def test_invalid_aggregation_raises_unknown_dsl_error_051(
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

    # Exec/Assert
    with pytest.raises(UnknownDslObject):
        search = Search(index=test_index_name)
        search.aggs.metric(name="min_age", agg_type="minsssssss", field="age")
        search.using(test_os_client).execute()


def test_aggregating_a_missing_index_raises_transport_error_052(
    invalid_test_os_client: OpenSearch,
    test_index_name: str,
):
    # Exec/Assert
    with pytest.raises(TransportError):
        search = Search(index=test_index_name)
        search.aggs.metric(name="min_age", agg_type="min", field="age")
        search.using(invalid_test_os_client).execute()
