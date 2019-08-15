package api

import (
	"fmt"
	"net/http"
	"strings"

	"github.com/gorilla/mux"
	"github.com/influxdata/telegraf/plugins/inputs/mesos_statsd/containers"
)

type Route struct {
	Name        string
	Method      string
	Pattern     string
	HandlerFunc func(c containers.Controller) http.HandlerFunc
}

type Routes []Route

func NewRouter(c containers.Controller) *mux.Router {
	router := mux.NewRouter().StrictSlash(true)
	for _, route := range routes {
		var handler http.Handler
		handler = route.HandlerFunc(c)
		handler = Logger(handler, route.Name)

		router.
			Methods(route.Method).
			Path(route.Pattern).
			Name(route.Name).
			Handler(handler)
	}

	return router
}

func Index(_ containers.Controller) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=UTF-8")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Not Found")
	}
}

var routes = Routes{
	Route{
		"Index",
		"GET",
		"/",
		Index,
	},

	Route{
		"ListContainers",
		strings.ToUpper("Get"),
		"/containers",
		ListContainers,
	},

	Route{
		"DescribeContainer",
		strings.ToUpper("Get"),
		"/container/{id}",
		DescribeContainer,
	},

	Route{
		"AddContainer",
		strings.ToUpper("Post"),
		"/container",
		AddContainer,
	},

	Route{
		"RemoveContainer",
		strings.ToUpper("Delete"),
		"/container/{id}",
		RemoveContainer,
	},
}
