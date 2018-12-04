// Gen is intended to be called via go generate from the root of the
// dcos_containers plugin directory. It finds every json fixture in the testdata
// directory and serializes it as protobuf.
//
// You should run 'go generate' every time you change one of the json files in
// the testdata directory, and commit both the changed json file and the
// changed binary file.

package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"

	"github.com/mesos/mesos-go/api/v1/lib/agent"
)

func main() {
	err := filepath.Walk("./testdata", func(fPath string, info os.FileInfo, err error) error {
		barf(err)
		if info.IsDir() {
			return nil
		}

		fName := info.Name()
		if filepath.Ext(fName) == ".json" {
			oPath := fPath[:len(fPath)-4] + "bin"
			log.Println("Converting", fPath, "to proto as", oPath)

			var buf agent.Response
			jsonData, err := ioutil.ReadFile(fPath)
			barf(err)

			err = json.Unmarshal(jsonData, &buf)
			barf(err)

			protoData, err := buf.Marshal()
			err = ioutil.WriteFile(oPath, protoData, 0644)
			barf(err)
		}

		return nil
	})
	barf(err)
	log.Println("Conversion complete.")
}

// barf will panic if an error occurred
func barf(err error) {
	if err != nil {
		panic(err)
	}
}
