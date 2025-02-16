// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package persist contains persist-specific interfaces.
package persist

import (
	"time"
)

// MarshallableItem is an interface for representing items that can be marshalled (for persisting).
type MarshallableItem interface {
	Marshal() ([]byte, error)
	ID() (string, error)
	Start() (time.Time)
	End() (time.Time)
}

// ItemSyncer is a syncer of items from/to some kind of storage, such as a database.
type ItemSyncer interface {
	Write(MarshallableItem) error
	Delete(MarshallableItem) error
	LoadAll() ([]MarshallableItem, error)
	LoadByID(string) (MarshallableItem, error)
}

// Unmarshaller is an interface for unmarshalling persisted items.
type Unmarshaller interface {
	Unmarshal([]byte) (MarshallableItem, error)
}
