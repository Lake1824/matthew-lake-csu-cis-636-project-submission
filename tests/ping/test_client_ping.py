from opensearchpy import OpenSearch


def test_client_successfully_pings_cluster_001(test_os_client: OpenSearch) -> None:
    # Exec
    result = test_os_client.ping()

    # Assert
    assert result is True


def test_client_cannot_ping_cluster_due_to_missing_cluster_002(
    invalid_test_os_client: OpenSearch,
) -> None:
    # Exec
    result = invalid_test_os_client.ping()

    # Assert
    assert result is False
