package persistdb

import (
	"encoding/json"
	"fmt"
	"sort"
	"log"
	//"reflect"

	//"github.com/golang-collections/go-datastructures/queue"
	"github.com/pkg/errors"
	"github.com/google/simhospital/pkg/state/persist"
	bolt "github.com/coreos/bbolt"
	"github.com/google/simhospital/pkg/state"
)

const (
	MessageSyncer int = 0
	EventSyncer       = 1
	PatientSyncer     = 2
)

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
			fmt.Println( fmt.Errorf("create bucket: %s", err1))
		} 
		_, err2 := tx.CreateBucket([]byte("Event"))
		if err2 != nil {
			fmt.Println( fmt.Errorf("create bucket: %s", err2))
		} 
		_, err3 := tx.CreateBucket([]byte("Patient"))
		if err3 != nil {
			fmt.Println( fmt.Errorf("create bucket: %s", err3))
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
    //fmt.Println("PersistDB: WRITE - " + str + " - " + id)//reflect.TypeOf(item).String())//+ " - " + string(b))
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
	//fmt.Println("PersistDB: DELETE " + str + " - " + id)
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	//delete(s.m, id)
	db.Update(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte(str))
		err := bucket.Delete([]byte(id))
		return err
	})
	return nil
}

// type DataMember interface {
//     state.HL7Message | state.Event | state.Patient
// }

func GetAllDataFromBucket[T persist.MarshallableItem](s *DbItemSyncer) ([]persist.MarshallableItem, error) {
	bucket := s.getSyncType()
	//fmt.Println("LOADALL " + bucket)
	str := s.getSyncType()
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
				//fmt.Printf("key=%s\n", k)
				data[string(k)] = dat
			} else {
				//fmt.Println(jerr)
			}
			return nil
		})
		return nil
	})

	//fmt.Println("SORT")
	keys := make([]string, 0)
	for id := range data {
		keys = append(keys, id)
	}
	sort.Strings(keys)

	sorted := make([]persist.MarshallableItem, len(keys))
	for i, k := range keys {
		sorted[i] = data[k]
		//fmt.Println(data[k].ID())
	}
	return sorted, nil

}

// LoadAll returns a slice of all the items in the map, sorted by id.
func (s *DbItemSyncer) LoadAll() ([]persist.MarshallableItem, error) {
	str := s.getSyncType()
	if (str == "HL7Message") {
		//fmt.Println("TEST1")
        data, _ := GetAllDataFromBucket[state.HL7Message](s)
		return data, nil
	} else if (str == "Event") {
		//fmt.Println("TEST2")
        data, _ := GetAllDataFromBucket[state.Event](s)
		return data, nil
	} else {
		//fmt.Println("TEST")
        data, _ := GetAllDataFromBucket[state.Patient](s)
		return data, nil
	}
}

// LoadByID returns an item in the map with the provided id, if it exists.
func (s *DbItemSyncer) LoadByID(id string) (persist.MarshallableItem, error) {
	str := s.getSyncType()
    //fmt.Println("PersistDB: LOADBYID " + str + " - " + id)
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
		fmt.Println(dat.ID())
		return nil
	})
	return dat, nil
}

// Count returns number of elements in the syncer for testing.
func (s *DbItemSyncer) Count() int {
	str := s.getSyncType()
    //fmt.Println("PersistDB: COUNT " + str)
	return len(s.m)
}

// CountDeletes returns the number of deletions requested.
func (s *DbItemSyncer) CountDeletes() int {
	str := s.getSyncType()
    //fmt.Println("PersistDB: COUNTDELETES " + str)
	return s.nDeletes
}

