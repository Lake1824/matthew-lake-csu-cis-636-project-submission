import pytest
from opensearchpy import (
    Search,
    OpenSearch,
    TransportError,
    UnknownDslObject,
)
from opensearchpy.helpers.query import Q


def test_full_text_query_returns_results_041(
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
    name_to_find_in_documents: str = "Bob"
    full_text_match_query: Search = Search(index=test_index_name).query(
        Q("match", name=name_to_find_in_documents)
    )
    search_results: dict = full_text_match_query.using(test_os_client).execute()[
        "hits"
    ]["hits"]

    bob_document: dict = search_results[0]["_source"]

    # Assert
    assert len(search_results) == 1
    assert bob_document["name"] == "Bob"
    assert bob_document["id"] == 2


def test_term_level_query_returns_results_042(
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
    ages_to_find_in_documents: list[int] = [30, 40]
    term_query: Search = Search(index=test_index_name).query(
        Q("terms", age=ages_to_find_in_documents)
    )
    search_results: dict = term_query.using(test_os_client).execute()["hits"]["hits"]

    # Assert
    assert len(search_results) == 2
    assert search_results[0]["_source"]["name"] == "Alice" or "Charlie"
    assert search_results[1]["_source"]["name"] == "Alice" or "Charlie"


def test_compound_bool_query_returns_results_043(
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
    height_to_find_in_documents: int = 5
    age_to_find_in_documents: int = 25
    compound_bool_query: Search = Search(index=test_index_name).query(
        Q(
            "bool",
            must=[
                Q("term", height=height_to_find_in_documents),
                Q("term", age=age_to_find_in_documents),
            ],
        )
    )
    search_results: dict = compound_bool_query.using(test_os_client).execute()["hits"][
        "hits"
    ]

    bob_document: dict = search_results[0]["_source"]

    # Assert
    assert len(search_results) == 1
    assert bob_document["name"] == "Bob"
    assert bob_document["id"] == 2


def test_geo_query_returns_results_044(
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
    polygon_around_ohio_city: list[list[float]] = [
        [-81.72870913010868, 41.4910168033276],
        [-81.69165529185678, 41.49316684427734],
        [-81.68826303905905, 41.482415925932614],
        [-81.72453404974227, 41.480852007377024],
        [-81.72870913010868, 41.4910168033276],
    ]

    geo_query: Search = Search(index=test_index_name).query(
        "nested",
        path="house_location",
        query=Q(
            "geo_shape",
            house_location__location={
                "shape": {
                    "type": "polygon",
                    "coordinates": [polygon_around_ohio_city],
                },
                "relation": "within",
            },
        ),
    )
    search_results: dict = geo_query.using(test_os_client).execute()["hits"]["hits"]

    # Assert
    assert len(search_results) == 1
    assert search_results[0]["_source"]["name"] == "Alice"


def test_general_match_all_query_returns_results_045(
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
    match_all_search_query: Search = Search(index=test_index_name).query(Q("match_all"))
    search_results: dict = match_all_search_query.using(test_os_client).execute()[
        "hits"
    ]["hits"]

    # Assert
    assert len(search_results) == len(test_index_documents)


def test_query_returns_no_results_046(
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
    match_all_search_query: Search = Search(index=test_index_name).query(Q("match_all"))
    search_results: dict = match_all_search_query.using(test_os_client).execute()[
        "hits"
    ]["hits"]

    # Assert
    assert len(search_results) == 0


def test_searching_a_missing_index_raises_transport_error_047(
    invalid_test_os_client: OpenSearch,
    test_index_name: str,
):
    # Exec/Assert
    with pytest.raises(TransportError):
        match_all_search_query: Search = Search(index=test_index_name).query(
            Q("match_all")
        )
        match_all_search_query.using(invalid_test_os_client).execute()["hits"]["hits"]


def test_invalid_query_raises_unknown_dsl_error_048(
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
        match_all_search_query: Search = Search(index=test_index_name).query(
            Q("match_alllllllllll")
        )
        match_all_search_query.using(test_os_client).execute()["hits"]["hits"]
