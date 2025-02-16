package persistazure

import (
	//"encoding/json"
	//"sort"

	"github.com/pkg/errors"
	"github.com/Arend-melissant/simhospital/pkg/logging"
	"github.com/Arend-melissant/simhospital/pkg/state/persist"
	"github.com/Arend-melissant/simhospital/pkg/state/persist/cosmosdb"

	//"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/data/azcosmos"
	//bolt "github.com/coreos/bbolt"
)

var endpoint = "https://calidosfhircosmosdb.documents.azure.com:443/"
var key = "RAjirMQfTXKPRiBuK1Jpz6V44Kw0AGAQXL2yN9P0206BN27Rn6cBimRbahIFlnuv23dFYFAHgMtpGFqi0hUVmw=="
var databaseName = "calidosfhirconvert"
var containerName = "Data"
var partitionKey = "/itemType"

var client *azcosmos.Client 

const (
	MessageSyncer int = 0
	EventSyncer       = 1
	PatientSyncer     = 2
)

var log = logging.ForCallerPackage()

// ItemSyncer implements the persist.ItemSyncer interface using a map.
// It tracks the LoadByID requests made to the syncer in the form of an
// internal `reqs` map for testing purposes.
type DbItemSyncer struct {
	m    map[string]persist.MarshallableItem
	// delete indicates whether to delete items or not.
	delete   bool
	nDeletes int
	syncType int
}

func serviceInit() { 
	cred, err := azcosmos.NewKeyCredential(key)
	if err != nil {
		log.Fatal("Failed to create a credential: ", err)
	}

	var clientErr error
	// Create a CosmosDB client
	client, clientErr = azcosmos.NewClientWithKey(endpoint, cred, nil)
	if clientErr != nil {
		log.Fatal("Failed to create cosmos db client: ", err)
	}
	
	err = cosmosdb.CreateDatabase(client, databaseName)
	if err != nil {
		log.Fatal("createDatabase failed: %s\n", err)
	}

	err = cosmosdb.CreateContainer(client, databaseName, containerName, partitionKey)
	if err != nil {
		log.Fatal("CreateContainer failed: %s\n", err)
	}

}

func open(tabel string) { 
}

// NewItemSyncer initializes the ItemSyncer.
func NewItemSyncer(syncType int) *DbItemSyncer {
	serviceInit()
	return &DbItemSyncer{m: map[string]persist.MarshallableItem{}, syncType: syncType}
}

// NewItemSyncerWithDelete initializes the ItemSyncer with a value for whether to delete items or not.
func NewItemSyncerWithDelete(syncType int, delete bool) *DbItemSyncer {
	return &DbItemSyncer{m: map[string]persist.MarshallableItem{}, delete: delete, syncType: syncType}
}

func (s *DbItemSyncer) getSyncType() string {
	var str string 
	str = "unknown"

	switch s.syncType {
	case PatientSyncer:
		str = "Patient" 
	case EventSyncer:
		str = "Event" 
	case MessageSyncer:
		str = "HL7Message" 
		default:
	}

	return str
}

//func storeItem(s *DbItemSyncer, )

// Write writes an item to the map.
func (s *DbItemSyncer) Write(item persist.MarshallableItem) error {
	str := s.getSyncType()
	id, err := item.ID()
	if err != nil {
		return errors.Wrap(err, "cannot get ID")
	}
	log.Infof("PersistDB: WRITE - %s - %s - %d - %d",str,id, item.Start().Unix(), item.End().Unix())

	cosmosdb.CreateItem(client, databaseName, containerName, str, id, item /* []byte(b)*/, item.Start(), item.End())
	return nil
}

// Delete deletes an item from the map.
func (s *DbItemSyncer) Delete(item persist.MarshallableItem) error {
	if !s.delete {
		return nil
	}
	str := s.getSyncType()
	s.nDeletes++
	id, err := item.ID()
	if err != nil {
		return errors.Wrap(err, "cannot get ID")
	}
	log.Infof("PersistDB: DELETE - %s - %s",str,id)

	err = cosmosdb.DeleteItem(client, databaseName, containerName, str, id)

	return err
}

// LoadAll returns a slice of all the items in the map, sorted by id.
func (s *DbItemSyncer) LoadAll() ([]persist.MarshallableItem, error) {
	str := s.getSyncType()
	switch str {
		case "Patient":
			data, _ := cosmosdb.ReadItems(client, databaseName, containerName, str)
			return data, nil
		case "Event":
			data, _ := cosmosdb.ReadItems(client, databaseName, containerName, str)
			return data, nil
		case "HL7Message":
			data, _ := cosmosdb.ReadItems(client, databaseName, containerName, str)
			return data, nil
		}
	return nil, nil
}

// LoadByID returns an item in the map with the provided id, if it exists.
func (s *DbItemSyncer) LoadByID(id string) (persist.MarshallableItem, error) {
	str := s.getSyncType()
	log.Infof("PersistDB: LOADBYID - %s - %s",str,id)
	
	dat, err := cosmosdb.ReadItem(client, databaseName, containerName, str, id)
	return dat, err
}

// Count returns number of elements in the syncer for testing.
func (s *DbItemSyncer) Count() int {
	//str := s.getSyncType()
    //fmt.Println("PersistDB: COUNT " + str)
	return len(s.m)
}

// CountDeletes returns the number of deletions requested.
func (s *DbItemSyncer) CountDeletes() int {
	//str := s.getSyncType()
    //fmt.Println("PersistDB: COUNTDELETES " + str)
	return s.nDeletes
}

