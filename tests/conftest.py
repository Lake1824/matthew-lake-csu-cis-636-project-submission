import time

import pytest
from opensearchpy import OpenSearch, RequestsHttpConnection


@pytest.fixture
def test_os_client() -> OpenSearch:
    client: OpenSearch = OpenSearch(
        hosts=[{"host": "test-opensearch-cluster", "port": 9200}],
        connection_class=RequestsHttpConnection,
    )

    return client


@pytest.fixture
def invalid_test_os_client() -> OpenSearch:
    client: OpenSearch = OpenSearch(
        hosts=[{"host": "invalid_host", "port": 9200}],
        connection_class=RequestsHttpConnection,
    )

    return client


@pytest.fixture
def test_index_name() -> str:
    return "test-index"


@pytest.fixture
def test_index_settings() -> dict:
    return {
        "index.number_of_shards": 1,
        "index.number_of_replicas": 1,
        "refresh_interval": "1s",
        "translog.durability": "async",
    }


@pytest.fixture
def test_index_mapping() -> dict:
    return {
        "dynamic": "strict",
        "properties": {
            "id": {"type": "integer"},
            "name": {"type": "text"},
            "age": {"type": "integer"},
            "height": {"type": "integer"},
            "house_location": {
                "type": "nested",
                "properties": {
                    "location": {"type": "geo_shape"},
                    "city": {"type": "text"},
                },
            },
        },
    }


@pytest.fixture
def test_index_documents() -> list[dict]:
    return [
        {
            "id": 1,
            "name": "Alice",
            "age": 30,
            "height": 6,
            "house_location": [
                {
                    "location": {
                        "type": "point",
                        "coordinates": [
                            -81.70783372827663,
                            41.48964855829594,
                        ],
                    },
                    "city": "Ohio City",
                }
            ],
        },
        {
            "id": 2,
            "name": "Bob",
            "age": 25,
            "height": 5,
            "house_location": [
                {
                    "location": {
                        "type": "point",
                        "coordinates": [-81.76002223285678, 41.483393355863555],
                    },
                    "city": "Edgewater",
                }
            ],
        },
        {
            "id": 3,
            "name": "Charlie",
            "age": 40,
            "height": 6,
            "house_location": [
                {
                    "location": {
                        "type": "point",
                        "coordinates": [-81.7996854963377, 41.48222043817722],
                    },
                    "city": "Lakewood",
                }
            ],
        },
        {
            "id": 4,
            "name": "Diana",
            "age": 22,
            "height": 4,
            "house_location": [
                {
                    "location": {
                        "type": "point",
                        "coordinates": [-81.75297678473846, 41.43039539944264],
                    },
                    "city": "Brooklyn",
                }
            ],
        },
        {
            "id": 5,
            "name": "Ethan",
            "age": 35,
            "height": 5,
            "house_location": [
                {
                    "location": {
                        "type": "point",
                        "coordinates": [-81.58153754719264, 41.53302739685439],
                    },
                    "city": "East Cleveland",
                }
            ],
        },
    ]


@pytest.fixture(autouse=True)
def delete_index_after_test(test_os_client: OpenSearch, test_index_name: str):
    # Run test case
    yield

    # Cleanup after each test
    test_os_client.indices.delete(index=test_index_name, ignore_unavailable=True)

    # Wait for index to be deleted
    while test_os_client.indices.exists(index=test_index_name):
        time.sleep(0.1)
