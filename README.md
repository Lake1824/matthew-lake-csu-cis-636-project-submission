# matthew-lake-csu-cis-636-project-submission

## Description
This repo is an automatic test suite for testing the Python library [opensearch-py](https://github.com/opensearch-project/opensearch-py) and OpenSearch.
The tests are integration tests, testing the library and the OpenSearch cluster itself.

### Test Categories
- Initializing an OpenSearch cluster client and using ping to verify connection
- Creating a cluster index
- Modifying a cluster index
- Deleting a cluster index
- Reindexing a cluster index
- Creating an index alias
- Updating an index alias
- Deleting an index alias
- Creating and indexing a document
- Bulk indexing documents
- Retrieving a document by its identifier
- Updating a document
- Bulk updating documents
- Deleting a document
- Searching for documents
- Aggregating documents

## How to Run
1. Download and setup [Docker Desktop](https://www.docker.com/products/docker-desktop/)
2. Setup to run the make commands
    - For Windows: Look at this stack overflow [thread](https://stackoverflow.com/questions/32127524/how-to-install-and-use-make-in-windows)
    - For Mac: Download and setup [Xcode](https://developer.apple.com/xcode/)
3. Clone this repo
4. In your terminal navigate to `matthew-lake-csu-cis-636-project-submission` project directory
5. Run the command `make test`