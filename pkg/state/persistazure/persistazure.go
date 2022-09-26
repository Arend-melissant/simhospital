package persistazure

import (
	"encoding/json"
	"sort"
	"github.com/pkg/errors"
	"github.com/google/simhospital/pkg/state/persist"
	bolt "github.com/coreos/bbolt"
	"github.com/google/simhospital/pkg/state"
	"github.com/google/simhospital/pkg/logging"
	"github.com/Azure/azure-sdk-for-go/sdk/data/aztables"
)

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

func open *ServiceClient {
	connStr := "DefaultEndpointsProtocol=https;AccountName=<myAccountName>;AccountKey=<myAccountKey>;EndpointSuffix=core.windows.net"
    serviceClient, err := aztables.NewServiceClientFromConnectionString(connStr, nil)
    if err != nil {
        panic(err)
    }
	return serviceClient
}

// NewItemSyncer initializes the ItemSyncer.
func NewItemSyncer(syncType int) *DbItemSyncer {
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Update(func(tx *bolt.Tx) error {
		_, err1 := tx.CreateBucket([]byte("HL7Message"))
		if err1 != nil {
			log.Errorf("create bucket: %s", err1)
		} 
		_, err2 := tx.CreateBucket([]byte("Event"))
		if err2 != nil {
			log.Errorf("create bucket: %s", err2)
		} 
		_, err3 := tx.CreateBucket([]byte("Patient"))
		if err3 != nil {
			log.Errorf("create bucket: %s", err3)
		} 
		
		return nil
	})

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
	b,_ := json.Marshal(item)
	str := s.getSyncType()
	id, err := item.ID()
	if err != nil {
		return errors.Wrap(err, "cannot get ID")
	}
	log.Infof("PersistDB: WRITE - %s - %s",str,id)
	// s.m[id] = item
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(str))
		err := bucket.Put([]byte(id), []byte(b))
		return err
	})
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
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(str))
		err := bucket.Delete([]byte(id))
		return err
	})
	return nil
}


func GetAllDataFromBucket[T persist.MarshallableItem](s *DbItemSyncer) ([]persist.MarshallableItem, error) {
	str := s.getSyncType()
	log.Infof("LOADALL - %s", str)
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	data := make(map[string]T)
	db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket([]byte(str))
	
		b.ForEach(func(k, v []byte) error {
			var dat T
			jerr := json.Unmarshal(v, &dat)
			if jerr == nil {
				data[string(k)] = dat
			} else {
				//fmt.Println(jerr)
			}
			return nil
		})
		return nil
	})

	keys := make([]string, 0)
	for id := range data {
		keys = append(keys, id)
	}
	sort.Strings(keys)

	sorted := make([]persist.MarshallableItem, len(keys))
	for i, k := range keys {
		sorted[i] = data[k]
	}
	return sorted, nil

}

// LoadAll returns a slice of all the items in the map, sorted by id.
func (s *DbItemSyncer) LoadAll() ([]persist.MarshallableItem, error) {
	str := s.getSyncType()
	if (str == "HL7Message") {
        data, _ := GetAllDataFromBucket[state.HL7Message](s)
		return data, nil
	} else if (str == "Event") {
        data, _ := GetAllDataFromBucket[state.Event](s)
		return data, nil
	} else {
        data, _ := GetAllDataFromBucket[state.Patient](s)
		return data, nil
	}
}

// LoadByID returns an item in the map with the provided id, if it exists.
func (s *DbItemSyncer) LoadByID(id string) (persist.MarshallableItem, error) {
	str := s.getSyncType()
	log.Infof("PersistDB: LOADBYID - %s - %s",str,id)
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	
	var dat state.Patient
	db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(str))
		v := b.Get([]byte(id))
		if jerr := json.Unmarshal(v, &dat); err != nil {
			panic(jerr)
		}
		return nil
	})
	return dat, nil
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

