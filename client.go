package elasticsearch

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"io/ioutil"
	"net/http"
	"net/url"
	"strings"
)

// Searcher set the contract to manage indices, synchronize data and request
type Searcher interface {
	CreateIndex(indexName, mapping string) (*Response, error)
	DeleteIndex(indexName string) (*Response, error)
	UpdateIndexSetting(indexName, mapping string) (*Response, error)
	IndexSettings(indexName string) (Settings, error)
	IndexExists(indexName string) (bool, error)
	Status(indices string) (*Settings, error)
	InsertDocument(indexName, documentType, identifier string, data []byte) (*InsertDocument, error)
	Document(indexName, documentType, identifier string) (*Document, error)
	DeleteDocument(indexName, documentType, identifier string) (*Document, error)
	Bulk(data []byte) (*Bulk, error)
	Search(indexName, documentType, data string) (*SearchResult, error)
	MSearch(queries []MSearchQuery) (*MSearchResult, error)
	Suggest(indexName, data string) ([]byte, error)
}

// A SearchClient describes the client configuration to manage an ElasticSearch index.
type SearchClient struct {
	Host url.URL
}

// NewSearchClient creates and initializes a new ElasticSearch client, implements core api for Indexing and searching.
func NewSearchClient(scheme, host, port string) Searcher {
	u := url.URL{
		Scheme: scheme,
		Host:   host + ":" + port,
	}
	return &SearchClient{Host: u}
}

// CreateIndex instantiates an index
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-create-index.html
func (client *SearchClient) CreateIndex(indexName, mapping string) (*Response, error) {
	url := client.Host.String() + "/" + indexName
	reader := bytes.NewBufferString(mapping)
	response, err := sendHTTPRequest("POST", url, reader)
	if err != nil {
		return &Response{}, err
	}

	esResp := &Response{}
	err = json.Unmarshal(response, esResp)
	if err != nil {
		return &Response{}, err
	}

	return esResp, nil
}

// DeleteIndex deletes an existing index.
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-delete-index.html
func (client *SearchClient) DeleteIndex(indexName string) (*Response, error) {
	url := client.Host.String() + "/" + indexName
	response, err := sendHTTPRequest("DELETE", url, nil)
	if err != nil {
		return &Response{}, err
	}

	esResp := &Response{}
	err = json.Unmarshal(response, esResp)
	if err != nil {
		return &Response{}, err
	}

	return esResp, nil
}

// UpdateIndexSetting changes specific index level settings in real time
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-update-settings.html
func (client *SearchClient) UpdateIndexSetting(indexName, mapping string) (*Response, error) {
	url := client.Host.String() + "/" + indexName + "/_settings"
	reader := bytes.NewBufferString(mapping)
	response, err := sendHTTPRequest("PUT", url, reader)
	if err != nil {
		return &Response{}, err
	}

	esResp := &Response{}
	err = json.Unmarshal(response, esResp)
	if err != nil {
		return &Response{}, err
	}

	return esResp, nil
}

// IndexSettings allows to retrieve settings of index
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-get-settings.html
func (client *SearchClient) IndexSettings(indexName string) (Settings, error) {
	url := client.Host.String() + "/" + indexName + "/_settings"
	response, err := sendHTTPRequest("GET", url, nil)
	if err != nil {
		return Settings{}, err
	}

	type settingsArray map[string]Settings
	dec := json.NewDecoder(bytes.NewBuffer(response))
	var info settingsArray
	err = dec.Decode(&info)
	if err != nil {
		return Settings{}, err
	}

	return info[indexName], nil
}

// IndexExists allows to check if the index exists or not.
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/indices-exists.html
func (client *SearchClient) IndexExists(indexName string) (bool, error) {
	url := client.Host.String() + "/" + indexName
	httpClient := &http.Client{}
	newReq, err := httpClient.Head(url)
	if err != nil {
		return false, err
	}

	return newReq.StatusCode == http.StatusOK, nil
}

// Status allows to get a comprehensive status information
func (client *SearchClient) Status(indices string) (*Settings, error) {
	url := client.Host.String() + "/" + indices + "/_status"
	response, err := sendHTTPRequest("GET", url, nil)
	if err != nil {
		return &Settings{}, err
	}

	esResp := &Settings{}
	err = json.Unmarshal(response, esResp)
	if err != nil {
		return &Settings{}, err
	}

	return esResp, nil
}

// InsertDocument adds or updates a typed JSON document in a specific index, making it searchable
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-index_.html
func (client *SearchClient) InsertDocument(indexName, documentType, identifier string, data []byte) (*InsertDocument, error) {
	url := client.Host.String() + "/" + indexName + "/" + documentType + "/" + identifier
	reader := bytes.NewBuffer(data)
	response, err := sendHTTPRequest("POST", url, reader)
	if err != nil {
		return &InsertDocument{}, err
	}

	esResp := &InsertDocument{}
	err = json.Unmarshal(response, esResp)
	if err != nil {
		return &InsertDocument{}, err
	}

	return esResp, nil
}

// Document gets a typed JSON document from the index based on its id
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-get.html
func (client *SearchClient) Document(indexName, documentType, identifier string) (*Document, error) {
	url := client.Host.String() + "/" + indexName + "/" + documentType + "/" + identifier
	response, err := sendHTTPRequest("GET", url, nil)
	if err != nil {
		return &Document{}, err
	}

	esResp := &Document{}
	err = json.Unmarshal(response, esResp)
	if err != nil {
		return &Document{}, err
	}

	return esResp, nil
}

// DeleteDocument deletes a typed JSON document from a specific index based on its id
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-delete.html
func (client *SearchClient) DeleteDocument(indexName, documentType, identifier string) (*Document, error) {
	url := client.Host.String() + "/" + indexName + "/" + documentType + "/" + identifier
	response, err := sendHTTPRequest("DELETE", url, nil)
	if err != nil {
		return &Document{}, err
	}

	esResp := &Document{}
	err = json.Unmarshal(response, esResp)
	if err != nil {
		return &Document{}, err
	}

	return esResp, nil
}

// Bulk makes it possible to perform many index/delete operations in a single API call.
// This can greatly increase the indexing speed.
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-bulk.html
func (client *SearchClient) Bulk(data []byte) (*Bulk, error) {
	url := client.Host.String() + "/_bulk"
	reader := bytes.NewBuffer(data)
	response, err := sendHTTPRequest("POST", url, reader)
	if err != nil {
		return &Bulk{}, err
	}

	esResp := &Bulk{}
	err = json.Unmarshal(response, esResp)
	if err != nil {
		return &Bulk{}, err
	}

	return esResp, nil
}

// Search allows to execute a search query and get back search hits that match the query
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-delete.html
func (client *SearchClient) Search(indexName, documentType, data string) (*SearchResult, error) {
	if len(documentType) > 0 {
		documentType = documentType + "/"
	}

	url := client.Host.String() + "/" + indexName + "/" + documentType + "/_search"
	reader := bytes.NewBufferString(data)
	response, err := sendHTTPRequest("POST", url, reader)
	if err != nil {
		return &SearchResult{}, err
	}

	esResp := &SearchResult{}
	err = json.Unmarshal(response, esResp)
	if err != nil {
		return &SearchResult{}, err
	}

	return esResp, nil
}

// MSearch allows to execute a multi-search and get back result
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-multi-search.html
func (client *SearchClient) MSearch(queries []MSearchQuery) (*MSearchResult, error) {
	replacer := strings.NewReplacer("\n", " ")
	queriesList := make([]string, len(queries))
	for i, query := range queries {
		queriesList[i] = query.Header + "\n" + replacer.Replace(query.Body)
	}

	mSearchQuery := strings.Join(queriesList, "\n") + "\n" // Don't forget trailing \n
	url := client.Host.String() + "/_msearch"
	reader := bytes.NewBufferString(mSearchQuery)
	response, err := sendHTTPRequest("POST", url, reader)

	if err != nil {
		return &MSearchResult{}, err
	}

	esResp := &MSearchResult{}
	err = json.Unmarshal(response, esResp)
	if err != nil {
		return &MSearchResult{}, err
	}

	return esResp, nil
}

// Suggest allows basic auto-complete functionality.
// http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/search-suggesters-completion.html
func (client *SearchClient) Suggest(indexName, data string) ([]byte, error) {
	url := client.Host.String() + "/" + indexName + "/_suggest"
	reader := bytes.NewBufferString(data)
	response, err := sendHTTPRequest("POST", url, reader)
	return response, err
}

func sendHTTPRequest(method, url string, body io.Reader) ([]byte, error) {
	client := &http.Client{}
	req, err := http.NewRequest(method, url, body)
	if err != nil {
		return nil, err
	}

	if method == "POST" || method == "PUT" {
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	}

	newReq, err := client.Do(req)
	if err != nil {
		return nil, err
	}

	defer newReq.Body.Close()
	response, err := ioutil.ReadAll(newReq.Body)
	if err != nil {
		return nil, err
	}

	if newReq.StatusCode > http.StatusCreated && newReq.StatusCode < http.StatusNotFound {
		return nil, errors.New(string(response))
	}

	return response, nil
}
