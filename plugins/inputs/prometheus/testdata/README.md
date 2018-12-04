* Fixtures

Each subdirectory of testdata represents a test case. When `go generate` is run
from the root of the plugin directory, appropriately named json files in these
subdirectories are loaded and output as binary-format protobuf files. 

See also:
 - [gen.go](../cmd/gen.go)
 - [mock-mesos-server](https://github.com/philipnrmn/mock-mesos-server)

