package elasticsearch

import "encoding/json"

// Response represents a boolean response sent back by the search egine
type Response struct {
	Acknowledged bool
	Error        string
	Status       int
}

// Settings represents the mapping structure of one or several indices
type Settings struct {
	Shards  map[string]interface{} `json:"_shards"`
	Indices map[string]interface{} `json:"indices"`
}

// Status represents the status of the search engine
type Status struct {
	TagLine string
	Version struct {
		Number         string
		BuildHash      string `json:"build_hash"`
		BuildTimestamp string `json:"build_timestamp"`
		BuildSnapshot  bool   `json:"build_snapshot"`
		LuceneVersion  string `json:"lucene_version"`
	}
	Name   string
	Status int
	Ok     bool
}

// InsertDocument represents the result of the insert operation of a document
type InsertDocument struct {
	Created bool   `json:"created"`
	Index   string `json:"_index"`
	Type    string `json:"_type"`
	ID      string `json:"_id"`
	Version int    `json:"_version"`
}

// Document represents a document
type Document struct {
	Index   string          `json:"_index"`
	Type    string          `json:"_type"`
	ID      string          `json:"_id"`
	Version int             `json:"_version"`
	Found   bool            `json:"found"`
	Source  json.RawMessage `json:"_source"`
}

// Bulk represents the result of the Bulk operation
type Bulk struct {
	Took   uint64 `json:"took"`
	Errors bool   `json:"errors"`
	Items  []struct {
		Create struct {
			Index  string `json:"_index"`
			Type   string `json:"_type"`
			ID     string `json:"_id"`
			Status int    `json:"status"`
			Error  string `json:"error"`
		} `json:"create"`
		Index struct {
			Index   string `json:"_index"`
			Type    string `json:"_type"`
			ID      string `json:"_id"`
			Version int    `json:"_version"`
			Status  int    `json:"status"`
			Error   struct {
				Type       string `json:"status"`
				Reason     string `json:"reason"`
				Index_UUID string `json:"index_uuid"`
				Shard      string `json:"shard"`
				Index      string `json:"index"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}

// SearchResult represents the result of the search operation
type SearchResult struct {
	Took     uint64 `json:"took"`
	TimedOut bool   `json:"timed_out"`
	Shards   struct {
		Total      int `json:"total"`
		Successful int `json:"successful"`
		Skipped    int `json:"skipped"`
		Failed     int `json:"failed"`
	} `json:"_shards"`
	Hits         ResultHits      `json:"hits"`
	Aggregations json.RawMessage `json:"aggregations"`
}

// ResultHits represents the result of the search hits
type ResultHits struct {
	Total struct {
		Value    int    `json:"value"`
		Relation string `json:"relation"`
	} `json:"total"`
	MaxScore float32 `json:"max_score"`
	Hits     []Hit   `json:"hits"`
}

type Hit struct {
	Index     string              `json:"_index"`
	Type      string              `json:"_type"`
	ID        string              `json:"_id"`
	Score     float32             `json:"_score"`
	Source    json.RawMessage     `json:"_source"`
	Highlight map[string][]string `json:"highlight,omitempty"`
}

// MSearchQuery Multi Search query
type MSearchQuery struct {
	Header string // index name, document type
	Body   string // query related to the declared index
}

// MSearchResult Multi search result
type MSearchResult struct {
	Responses []SearchResult `json:"responses"`
}

type UpdateByQueryResult struct {
	Took             int  `json:"took"`
	TimedOut         bool `json:"timed_out"`
	Total            int  `json:"total"`
	Updated          int  `json:"updated"`
	Deleted          int  `json:"deleted"`
	Batches          int  `json:"batches"`
	VersionConflicts int  `json:"version_conflicts"`
	Noops            int  `json:"noops"`
	Retries          struct {
		Bulk   int `json:"bulk"`
		Search int `json:"search"`
	} `json:"retries"`
	ThrottledMillis      int           `json:"throttled_millis"`
	RequestsPerSecond    int           `json:"requests_per_second"`
	ThrottledUntilMillis int           `json:"throttled_until_millis"`
	Failures             []interface{} `json:"failures"`
}
