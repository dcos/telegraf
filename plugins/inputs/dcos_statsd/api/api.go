package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/influxdata/telegraf/plugins/inputs/dcos_statsd/containers"
)

// ReportHealth returns 200 OK if the API server is online
func ReportHealth(_ containers.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
	}
}

// ListContainers returns a list of all containers
func ListContainers(c containers.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		data, err := json.Marshal(c.ListContainers())
		if err != nil {
			log.Printf("E! Could not list containers: %s", err)
			w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "Could not list containers")
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}
}

// DescribeContainer returns a single container
func DescribeContainer(c containers.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		cid := vars["id"]

		ctr, ok := c.GetContainer(cid)
		if !ok {
			log.Printf("I! Could not find requested container %q", cid)
			w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "Container %q not found", cid)
			return
		}

		data, err := json.Marshal(ctr)
		if err != nil {
			log.Printf("E! could not encode json: %s", err)
			w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Could not describe container %s", cid)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		w.Write(data)
	}
}

// AddContainer adds a container and starts a statsd server. It returns the
// container definition include the server host and port.
func AddContainer(c containers.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var ctr containers.Container
		decoder := json.NewDecoder(r.Body)
		if err := decoder.Decode(&ctr); err != nil {
			log.Printf("E! could not decode json: %s", err)
			w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "Could not decode request")
			return
		}

		// If container already exists, redirect
		_, ok := c.GetContainer(ctr.Id)
		if ok {
			log.Printf("I! Could not add container %q as it already exists", ctr.Id)
			http.Redirect(w, r, "/container/"+ctr.Id, http.StatusSeeOther)
			return
		}

		result, err := c.AddContainer(ctr)
		if err != nil {
			log.Printf("E! could not add container: %s", err)
			w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Could not add container %s", ctr.Id)
			return
		}

		data, err := json.Marshal(result)
		if err != nil {
			log.Printf("E! could not encode json: %s", err)
			w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprintf(w, "Could not describe container %s", ctr.Id)
			return
		}

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusCreated)
		w.Write(data)
	}
}

// RemoveContainer removes the specified container and stops its statsd server
func RemoveContainer(c containers.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		cid := vars["id"]
		ctr, ok := c.GetContainer(cid)
		if !ok {
			log.Printf("I! Could not find requested container %q", cid)
			w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprintf(w, "Container %q not found", cid)
			return
		}

		// We perform the action later so as not to delay container cleanup
		w.Header().Set("Content-Type", "application/json; charset=UTF-8")
		w.WriteHeader(http.StatusAccepted)

		err := c.RemoveContainer(*ctr)
		if err != nil {
			log.Printf("E! could not remove container: %s", err)
			// No http response because we already sent 'accepted'
		}
	}
}
