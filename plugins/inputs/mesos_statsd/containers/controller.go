package containers

// Controller is the interface for controlling containers. We define it in order
// to pass a MesosStatsd instance into the API. We cannot directly require the
// mesos_statsd package without encountering a circular import.
type Controller interface {
	ListContainers() []Container
	GetContainer(cid string) (*Container, bool)
	AddContainer(c Container) (*Container, error)
	RemoveContainer(c Container) error
}
