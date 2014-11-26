# Elasticsearch

A minimalistic elasticsearch client in Go (golang).

This package is a basic wrapper which exposes Elasticsearch methods in GO. It helps you to stay focus on your queries and your index mapping by doing for you the communication with elasticsearch.


## Methods

Index management:

* CreateIndex
* DeleteIndex
* UpdateIndexSetting
* IndexSettings
* IndexExists
* Status

CRUD:

* InsertDocument
* Document
* DeleteDocument

Queries:

* Bulk
* Search
* Suggest

## Compatibility

Support all Elasticsearch versions


## Install

    go get github.com/maximelamure/elasticsearch


## Usage

Below is an example which shows some common use cases. Check [client_test.go](https://github.com/maximelamure/elasticsearch/blob/master/client_test.go) for more usage.

TBD
