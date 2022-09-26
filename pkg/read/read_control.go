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

// Package rate contains functionality to deal with rates.
package read

import (
	"fmt"
	"net/http"
	//"strconv"
	"encoding/json"

	"github.com/Arend-melissant/simhospital/pkg/logging"
	"github.com/Arend-melissant/simhospital/pkg/hospital"
)

var log = logging.ForCallerPackage()

// Controller is a rate controller.
type Controller struct {
	Hospital       *hospital.Hospital
}

// NewController creates a new Controller.
func NewController(h *hospital.Hospital) *Controller {
	return &Controller{
		Hospital: h,
	}
}

// ServeHTTP handles the requests made from the slider on the control dashboard.
func (c *Controller) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case "GET":
		id := r.URL.Query().Get("id")
		if id != "" {
			p := c.Hospital.GetPatient(id);
			if (p != nil) {
				b, err := json.Marshal(p.PatientInfo)
				if err != nil {
					fmt.Println(err)
					return
				}
				w.Write([]byte(b))
			} else {
				w.Write([]byte("Patient not found"))
			}
		} else {
			data := c.Hospital.GetAllPatients();
			if (data != nil) {
				b, err := json.Marshal(data)
				if err != nil {
					fmt.Println(err)
					return
				}
				w.Write([]byte(b))
			} else {
				w.Write([]byte("Patients not found"))
			}
		}
		
	default:
		http.Error(w, fmt.Sprintf("Unknown method: %q", r.Method), http.StatusInternalServerError)
	}
}

// func (c *Controller) handlePost(w http.ResponseWriter, r *http.Request) {
// 	defer r.Body.Close()

// 	body, err := ioutil.ReadAll(r.Body)
// 	errStr := "Failed to change the rate value"
// 	if err != nil {
// 		log.WithError(err).Warning(errStr)
// 		http.Error(w, "Error reading request body", http.StatusInternalServerError)
// 		return
// 	}

// 	sbody := string(body)
// 	prefix := "value="
// 	if !strings.HasPrefix(sbody, prefix) {
// 		log.Warningf("%s: missing prefix %q in request body: %q", errStr, prefix, sbody)
// 		http.Error(w, `Error extracting value: the request must be in the format "value=X"`, http.StatusInternalServerError)
// 		return
// 	}
// 	svalue := strings.TrimPrefix(sbody, prefix)
// 	f, err := strconv.ParseFloat(svalue, 64)
// 	if err != nil {
// 		log.WithError(err).Warning(errStr)
// 		http.Error(w, "Error parsing value to float", http.StatusInternalServerError)
// 		return
// 	}
// 	if f < 0 {
// 		msg := fmt.Sprintf("Invalid value: rate per minute must be greater that zero, but was: %v", f)
// 		log.Warningf("%s: %s", errStr, msg)
// 		http.Error(w, msg, http.StatusInternalServerError)
// 		return
// 	}
// 	// Sometimes we get a POST message even if the value didn't change.
// 	if c.rate != f {
// 		c.rate = f
// 		c.rateChanged <- true
// 	}
// }

// func (c *Controller) heartbeat() time.Duration {
// 	return time.Duration(float64(c.per) / c.rate)
// }

// // Heartbeat returns a duration between two pathways based on rate and per values.
// // If the rate is set to zero, returns the maximum duration value.
// func (c *Controller) Heartbeat() time.Duration {
// 	if c.rate == 0 {
// 		log.Infof("Rate set to %v / %v. Not generating pathway", c.rate, c.per)
// 		// Max Duration value.
// 		return time.Duration(1<<63 - 1)
// 	}
// 	h := c.heartbeat()
// 	log.Debugf("Rate set to %v / %v. Generating one pathway every %v", c.rate, c.per, h)
// 	return h
// }

// // RateChanged returns a channel, where the changes of the rate are signaled.
// func (c *Controller) RateChanged() <-chan bool {
// 	return c.rateChanged
// }

// // InitialElapsed returns a value of the heartbeat, if the rate is not zero.
// // Otherwise, returns zero.
// func (c *Controller) InitialElapsed() time.Duration {
// 	if c.rate > 0 {
// 		return c.heartbeat()
// 	}
// 	return 0
// }
