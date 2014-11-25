package elasticsearch

import (
	"bytes"
	"encoding/json"
	"testing"
	"time"
)

var (
	ProductDocumentType       = "PRODUCT"
	ESScheme                  = "http"
	ESHost                    = "localhost"
	ESPort                    = "9201"
	ESSearchIndexName         = "search"
	ESRecommendationIndexName = "recommendation"
	IndexName                 = "test"
	IndexMapping              = `{"settings": 
									{
										"number_of_shards" : 5, 
										"number_of_replicas" : 1
									}
								  }`
	SuggestionIndexName    = "test"
	SuggestionIndexMapping = `{
								  "mappings": {
								    "suggestion" : {
								      "properties" : {
								        "name_suggest" : {
								          "type" :     "completion",
								          "payloads" : true
								        }
								      }
								    }
								  }
								}`
)

func TestIndexManagement(t *testing.T) {
	helper := Test{}
	client := NewSearchClient(ESScheme, ESHost, ESPort)

	//If the index exists, remove it
	if response, _ := client.IndexExists(IndexName); response {
		delReponse, err := client.DeleteIndex(IndexName)
		helper.OK(t, err)
		helper.Assert(t, delReponse.Acknowledged, "Unable to remove existing index:"+delReponse.Error)
	}

	//Check if we have the test index
	response, err := client.IndexExists(IndexName)
	helper.OK(t, err)
	helper.Assert(t, !response, "Index has not been removed with the DeleteIndex function")

	//Create the index
	createResponse, err := client.CreateIndex(IndexName, IndexMapping)
	helper.OK(t, err)
	helper.Assert(t, createResponse.Acknowledged, "Index has not been created")

	//Check if the index has been created
	response, err = client.IndexExists(IndexName)
	helper.OK(t, err)
	helper.Assert(t, response, "Index has not been created with the CreateIndex function")

	//Delete the index
	deleteResponse, err := client.DeleteIndex(IndexName)
	helper.OK(t, err)
	helper.Assert(t, deleteResponse.Acknowledged, "Index has not been deleted")

	//Check if the index has been deleted
	response, err = client.IndexExists(IndexName)
	helper.OK(t, err)
	helper.Assert(t, !response, "Index has not been removed with DeleteIndex function")

}

func TestCRUD(t *testing.T) {
	type Product struct {
		Name string
		ID   string `json:"_id"`
	}

	helper := Test{}
	client := NewSearchClient(ESScheme, ESHost, ESPort)
	//Create the index
	client.CreateIndex(IndexName, IndexMapping)

	item := Product{Name: "Jeans", ID: "1234"}

	jsonProduct, err := json.Marshal(item)
	helper.OK(t, err)

	//Insert
	insertResponse, err := client.InsertDocument(IndexName, ProductDocumentType, item.ID, jsonProduct)
	helper.OK(t, err)
	helper.Assert(t, insertResponse.ID == "1234", "The document has not been inserted")

	version := insertResponse.Version

	//Update
	item.Name = "Polo"
	insertResponse, err = client.InsertDocument(IndexName, ProductDocumentType, item.ID, jsonProduct)
	helper.OK(t, err)
	helper.Assert(t, insertResponse.Version == version+1, "The document has not been updated")

	//Read
	readResponse, err := client.Document(IndexName, ProductDocumentType, item.ID)
	helper.OK(t, err)
	helper.Assert(t, readResponse.Found, "The document has not been found")

	var p Product
	err = json.Unmarshal(readResponse.Source, &p)
	helper.OK(t, err)
	helper.Assert(t, p.ID == "1234", "The document has not been retreived")

	//Delete
	delResponse, err := client.DeleteDocument(IndexName, ProductDocumentType, item.ID)
	helper.OK(t, err)
	helper.Assert(t, delResponse.Found, "The document has not beem deleted")

	//Delete the index
	deleteResponse, err := client.DeleteIndex(IndexName)
	helper.OK(t, err)
	helper.Assert(t, deleteResponse.Acknowledged, "Index has not been deleted")
}

func TestSearch(t *testing.T) {
	type Product struct {
		Name   string
		Colors []string
		ID     string `json:"_id"`
	}

	products := [...]Product{
		Product{Name: "Jeans", ID: "1", Colors: []string{"blue", "red"}},
		Product{Name: "Polo", ID: "2", Colors: []string{"yellow", "red"}},
		Product{Name: "Shirt", ID: "3", Colors: []string{"brown", "blue"}},
	}
	helper := Test{}
	client := NewSearchClient(ESScheme, ESHost, ESPort)
	client.CreateIndex(IndexName, IndexMapping)

	//Bulk
	var buffer bytes.Buffer
	for _, value := range products {
		buffer.WriteString(BulkIndexConstant(IndexName, ProductDocumentType, value.ID))
		buffer.WriteByte('\n')

		jsonProduct, err := json.Marshal(value)
		helper.OK(t, err)
		buffer.Write(jsonProduct)
		buffer.WriteByte('\n')
	}

	_, err := client.Bulk(buffer.Bytes())
	helper.OK(t, err)

	//We have to wait after a bulk
	time.Sleep(1500 * time.Millisecond)

	//Search
	search, err := client.Search(IndexName, ProductDocumentType, SearchByColorQuery("red"))
	helper.OK(t, err)
	helper.Assert(t, search.Hits.Total == 2, "The search doesn't return all matched items")

	//Delete the index
	deleteResponse, err := client.DeleteIndex(IndexName)
	helper.OK(t, err)
	helper.Assert(t, deleteResponse.Acknowledged, "Index has not been deleted")
}

func BulkIndexConstant(indexName, documentType, id string) string {

	return `{"index": 
				{ "_index": "` + indexName + `", 
				"_type": "` + documentType + `", 
				"_id": "` + id + `"
				}
			}`
}

func SearchByColorQuery(color string) string {
	return `{
			 	"query": {
					"match": {
						"Colors": "` + color + `"
					    }
					}
			}`
}

func TestSuggestion(t *testing.T) {

	type PayLoadSuggester struct {
		ID  string `json:"id"`
		SKU string `json:"sku"`
	}

	type InputSuggester struct {
		Input   []string         `json:"input"`
		Ouput   string           `json:"output"`
		Payload PayLoadSuggester `json:"payload"`
	}

	type SuggestionItem struct {
		Name InputSuggester `json:"name_suggest"`
	}

	type OutputSuggester struct {
		Text    string           `json:"text"`
		Score   float32          `json:"score"`
		Payload PayLoadSuggester `json:"payload"`
	}

	type SuggestionResult struct {
		Shards struct {
			Total      int `json:"total"`
			Successful int `json:"successful"`
			Failed     int `json:"failed"`
		} `json:"_shards"`
		Suggestion []struct {
			Text    string            `json:"text"`
			Offset  float32           `json:"offset"`
			Lenght  int               `json:"length"`
			Options []OutputSuggester `json:"options"`
		} `json:"suggestion"`
	}

	helper := Test{}
	client := NewSearchClient(ESScheme, ESHost, ESPort)
	client.CreateIndex(SuggestionIndexName, SuggestionIndexMapping)

	//Add Data
	sugg := &SuggestionItem{}
	sugg.Name = InputSuggester{}
	sugg.Name.Input = []string{"jeans", "Levi's jeans", "Levi's"}
	sugg.Name.Ouput = "Levi's jeans"
	sugg.Name.Payload = PayLoadSuggester{"12345", "HJYSTG"}

	jsonSuggestion, err := json.Marshal(sugg)
	helper.OK(t, err)

	//Insert
	insertResponse, err := client.InsertDocument(SuggestionIndexName, "suggestion", "1234", jsonSuggestion)
	helper.OK(t, err)
	helper.Assert(t, insertResponse.ID == "1234", "The document has not been inserted")

	//Suggest
	suggestResponse, err := client.Suggest(SuggestionIndexName, SuggestByTermQuery("jean"))
	helper.OK(t, err)

	var s SuggestionResult
	err = json.Unmarshal(suggestResponse, &s)
	helper.OK(t, err)
	helper.Assert(t, s.Shards.Failed == 0, "No suggestion inserted")

	//Delete the index
	deleteResponse, err := client.DeleteIndex(SuggestionIndexName)
	helper.OK(t, err)
	helper.Assert(t, deleteResponse.Acknowledged, "Index has not been deleted")

}

func SuggestByTermQuery(term string) string {
	return `{
				"suggestion" : {
					"text" : "` + term + `",
					"completion" : {
						"field" : "name_suggest"
					}
				}
			}`
}
