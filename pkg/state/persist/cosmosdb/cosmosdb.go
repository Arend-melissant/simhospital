package cosmosdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"
	//"strconv"

	"github.com/Arend-melissant/simhospital/pkg/state/persist"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	"github.com/Arend-melissant/simhospital/pkg/state"
)

type itemData struct {
	Id       string `json:"id"`
	ItemType string `json:"itemType"`
	Item     persist.MarshallableItem `json:"item"`
	Start    int64 `json:"start"`
	Stop     int64 `json:"stop"`
}

type messageItemData struct {
	Id       string `json:"id"`
	ItemType string `json:"itemType"`
	Item     state.HL7Message `json:"item"`
	Start    int64 `json:"start"`
	Stop     int64 `json:"stop"`
}

type eventItemData struct {
	Id       string `json:"id"`
	ItemType string `json:"itemType"`
	Item     state.Event `json:"item"`
	Start    int64 `json:"start"`
	Stop     int64 `json:"stop"`
}

type patientItemData struct {
	Id       string `json:"id"`
	ItemType string `json:"itemType"`
	Item     state.Patient `json:"item"`
	Start    int64 `json:"start"`
	Stop     int64 `json:"stop"`
}

func CreateDatabase(client *azcosmos.Client, databaseName string) error {
//	databaseName := "adventureworks"

	databaseProperties := azcosmos.DatabaseProperties{ID: databaseName}

	// This is a helper function that swallows 409 errors
	errorIs409 := func(err error) bool {
		var responseErr *azcore.ResponseError
		return err != nil && errors.As(err, &responseErr) && responseErr.StatusCode == 409
	}
	ctx := context.TODO()
	databaseResp, err := client.CreateDatabase(ctx, databaseProperties, nil)

	switch {
	case errorIs409(err):
		log.Printf("Database [%s] already exists\n", databaseName)
	case err != nil:
		return err
	default:
		log.Printf("Database [%v] created. ActivityId %s\n", databaseName, databaseResp.ActivityID)
	}
	return nil
}

func CreateContainer(client *azcosmos.Client, databaseName, containerName, partitionKey string) error {
//	databaseName = adventureworks
//	containerName = customer
//	partitionKey = "/customerId"

	databaseClient, err := client.NewDatabase(databaseName)
	if err != nil {
		return err
	}

	// creating a container
	containerProperties := azcosmos.ContainerProperties{
		ID: containerName,
		PartitionKeyDefinition: azcosmos.PartitionKeyDefinition{
			Paths: []string{partitionKey},
		},
	}

	// this is a helper function that swallows 409 errors
	errorIs409 := func(err error) bool {
		var responseErr *azcore.ResponseError
		return err != nil && errors.As(err, &responseErr) && responseErr.StatusCode == 409
	}

	// setting options upon container creation
	throughputProperties := azcosmos.NewManualThroughputProperties(400) //defaults to 400 if not set
	options := &azcosmos.CreateContainerOptions{
		ThroughputProperties: &throughputProperties,
	}
	ctx := context.TODO()
	containerResponse, err := databaseClient.CreateContainer(ctx, containerProperties, options)
	
	switch {
	case errorIs409(err):
		log.Printf("Container [%s] already exists\n", containerName)
	case err != nil:
		return err
	default:
		log.Printf("Container [%s] created. ActivityId %s\n", containerName, containerResponse.ActivityID)
	}
	return nil
}

func CreateItem(client *azcosmos.Client, databaseName string, containerName string, itemType string, id string, item persist.MarshallableItem, start time.Time, end time.Time) error {
	// create container client
	containerClient, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return fmt.Errorf("failed to create a container client: %s", err)
	}

	// specifies the value of the partiton key
	pk := azcosmos.NewPartitionKeyString(itemType)

	// setting the item options upon creating ie. consistency level
	itemOptions := azcosmos.ItemOptions{
		ConsistencyLevel: azcosmos.ConsistencyLevelSession.ToPtr(),
	}

	// this is a helper function that swallows 409 errors
	errorIs409 := func(err error) bool {
		var responseErr *azcore.ResponseError
		return err != nil && errors.As(err, &responseErr) && responseErr.StatusCode == 409
	}

	data := itemData{
		Id: id,
		ItemType:  itemType,
		Item:  item,
		Start: start.Unix(),
		Stop: end.Unix(),
	}

	marshalled, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("failed to create data: %s", err)
	}

	ctx := context.TODO()
	itemResponse, err := containerClient.UpsertItem(ctx, pk, marshalled, &itemOptions)
	
	if err != nil {
		log.Printf("Wite to db, error = %s", err)
	}

	switch {
	case errorIs409(err):
		log.Printf("Item with partitionkey value %s already exists\n", pk)
	case err != nil:
		return err
	default:
		//log.Printf("Status %d. Item %v created. ActivityId %s. Consuming %v Request Units.\n", itemResponse.RawResponse.StatusCode, pk, itemResponse.ActivityID, itemResponse.RequestCharge)
		_ = itemResponse
	}
	
	return nil
}

func ReadItem(client *azcosmos.Client, databaseName string, containerName string, partitionKey string, itemId string) (persist.MarshallableItem, error) {
	// Create container client
	containerClient, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create a container client: %s", err)
	}

	// Specifies the value of the partiton key
	pk := azcosmos.NewPartitionKeyString(partitionKey)

	// Read an item
	ctx := context.TODO()
	itemResponse, err := containerClient.ReadItem(ctx, pk, itemId, nil)
	if err != nil {
		return nil, err
	}

	var data persist.MarshallableItem
	var dataerr error

	switch(partitionKey){
		case "HL7Message":
			var itemResponseBody messageItemData
			dataerr = json.Unmarshal(itemResponse.Value, &itemResponseBody)
			if (err == nil)	{
				data = itemResponseBody.Item
			}
		case "Event":
			var itemResponseBody eventItemData
			dataerr = json.Unmarshal(itemResponse.Value, &itemResponseBody)
			if (err == nil)	{
				data = itemResponseBody.Item
			}
		case "Patient":
			var itemResponseBody patientItemData
			dataerr = json.Unmarshal(itemResponse.Value, &itemResponseBody)
			if (err == nil)	{
				data = itemResponseBody.Item
			}
	}
	if dataerr != nil {
		return nil, dataerr
	}

	return data, nil
}

func ReadItems(client *azcosmos.Client, databaseName string, containerName string, partitionKey string) ([]persist.MarshallableItem, error) {
	// Create container client
	containerClient, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return nil, fmt.Errorf("failed to create a container client: %s", err)
	}

	// Specifies the value of the partiton key
	pk := azcosmos.NewPartitionKeyString(partitionKey)

	result := make([]persist.MarshallableItem, 0)
	ctx := context.TODO()
	queryPager := containerClient.NewQueryItemsPager("select * from c where c.itemType = '"+ partitionKey + "' order by c.id", pk, nil)
	for queryPager.More() {
		queryResponse, err := queryPager.NextPage(ctx)
		if err != nil {
			log.Printf("ERROR: %s", err)
		}
	
		switch(partitionKey){
		case "HL7Message":
			for _, item := range queryResponse.Items {
				var itemResponseBody messageItemData
				json.Unmarshal(item, &itemResponseBody)
				result = append(result, itemResponseBody.Item)
			}
		case "Event":
			for _, item := range queryResponse.Items {
				var itemResponseBody eventItemData
				json.Unmarshal(item, &itemResponseBody)
				result = append(result, itemResponseBody.Item)
			}
		case "Patient":
			for _, item := range queryResponse.Items {
				var itemResponseBody patientItemData
				json.Unmarshal(item, &itemResponseBody)
				result = append(result, itemResponseBody.Item)
			}
		}
		
	}

	return result, nil
}

func DeleteItem(client *azcosmos.Client, databaseName, containerName, partitionKey, itemId string) error {
//	databaseName = "adventureworks"
//	containerName = "customer"
//	partitionKey = "1"
//	itemId = "1"

	// Create container client
	containerClient, err := client.NewContainer(databaseName, containerName)
	if err != nil {
		return fmt.Errorf("failed to create a container client:: %s", err)
	}
	// Specifies the value of the partiton key
	pk := azcosmos.NewPartitionKeyString(partitionKey)

	// Delete an item
	ctx := context.TODO()

	res, err := containerClient.DeleteItem(ctx, pk, itemId, nil)
	if err != nil {
		return err
	}



	//log.Printf("Status %d. Item %v deleted. ActivityId %s. Consuming %v Request Units.\n", res.RawResponse.StatusCode, pk, res.ActivityID, res.RequestCharge)
	_ = res

	return nil
}
