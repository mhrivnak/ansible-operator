package runner

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/sirupsen/logrus"
	"github.com/water-hole/ansible-operator/pkg/paramconv"
	"github.com/water-hole/ansible-operator/pkg/runner/eventapi"
	"github.com/water-hole/ansible-operator/pkg/runner/internal/inputdir"
	yaml "gopkg.in/yaml.v2"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

// Runner - a runnable that should take the parameters and name and namespace
// and run the correct code.
type Runner interface {
	Run(map[string]interface{}, string, string, string) (*eventapi.StatusJobEvent, error)
}

// watch holds data used to create a mapping of GVK to ansible playbook or role.
// The mapping is used to compose an ansible operator.
type watch struct {
	Name    string `yaml:"name"`
	Version string `yaml:"version"`
	Group   string `yaml:"group"`
	Kind    string `yaml:"kind"`
	Path    string `yaml:"path"`
}

// NewFromConfig reads the operator's config file at the provided path.
func NewFromConfig(path string) (map[schema.GroupVersionKind]Runner, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		logrus.Errorf("failed to get config file %v", err)
		return nil, err
	}
	watches := []watch{}
	err = yaml.Unmarshal(b, &watches)
	if err != nil {
		logrus.Errorf("failed to unmarshal config %v", err)
		return nil, err
	}

	m := map[schema.GroupVersionKind]Runner{}
	for _, w := range watches {
		s := schema.GroupVersionKind{
			Group:   w.Group,
			Version: w.Version,
			Kind:    w.Kind,
		}
		m[s] = NewForPlaybook(w.Path, s)
	}
	return m, nil
}

// NewForRole returns a new Runner based on the path to an ansible playbook.
func NewForPlaybook(path string, gvk schema.GroupVersionKind) Runner {
	return &runner{
		Path: path,
		GVK:  gvk,
		cmdFunc: func(ident, inputDirPath string) *exec.Cmd {
			dc := exec.Command("ansible-runner", "-vv", "-p", path, "-i", ident, "run", inputDirPath)
			dc.Stdout = os.Stdout
			dc.Stderr = os.Stderr
			return dc
		},
	}
}

// NewForRole returns a new Runner based on the path to an ansible role.
func NewForRole(path string, gvk schema.GroupVersionKind) Runner {
	return &runner{
		Path: path,
		GVK:  gvk,
		cmdFunc: func(ident, inputDirPath string) *exec.Cmd {
			// FIXME the below command does not fully work
			dc := exec.Command("ansible-runner", "-vv", "--role", "busybox", "--roles-path", "/opt/ansible/roles/", "--hosts", "localhost", "-i", ident, "run", inputDirPath)
			dc.Stdout = os.Stdout
			dc.Stderr = os.Stderr
			return dc
		},
	}
}

// runner - implements the Runner interface for a GVK that's being watched.
type runner struct {
	Path    string                                     // path on disk to a playbook or role depending on what cmdFunc expects
	GVK     schema.GroupVersionKind                    // GVK being watched that corresponds to the Path
	cmdFunc func(ident, inputDirPath string) *exec.Cmd // returns a Cmd that runs ansible-runner
}

// Run uses the runner with the given input and returns a status.
func (r *runner) Run(parameters map[string]interface{}, name, namespace, kubeconfig string) (*eventapi.StatusJobEvent, error) {
	parameters["meta"] = map[string]string{"namespace": namespace, "name": name}
	ident := strconv.Itoa(rand.Int())
	sockPath := fmt.Sprintf("/tmp/mysockPath-%s", ident)

	inputDir := inputdir.InputDir{
		Path:         filepath.Join("/tmp/ansible-operator/runner/", r.GVK.Group, r.GVK.Version, r.GVK.Kind, namespace, name),
		PlaybookPath: r.Path,
		Parameters:   paramconv.MapToSnake(parameters),
		EnvVars: map[string]string{
			"K8S_AUTH_KUBECONFIG": kubeconfig,
		},
		Settings: map[string]string{
			"runner_http_url":  sockPath,
			"runner_http_path": "/events/",
		},
	}
	err := inputDir.Write()
	if err != nil {
		return nil, err
	}

	// errChan gives both the http server and ansible-runner command, each of
	// which runs in a goroutine, an opportunity to return an error and have it
	// sent back to the control loop below.
	errChan := make(chan error, 2)

	receiver, err := eventapi.New(sockPath, "/events/", errChan)
	if err != nil {
		return nil, err
	}
	defer receiver.Close()

	dc := r.cmdFunc(ident, inputDir.Path)
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		localErr := dc.Run()
		if localErr != nil {
			logrus.Errorf("error for run %s: %s", ident, localErr.Error())
		} else {
			logrus.Infof("ansible-runner exited successfully for run %s", ident)
		}
		errChan <- localErr
		wg.Done()
	}()
	// ensure ansible-runner exits before the http server gets closed
	defer wg.Wait()
	// once this function returns, immediately stop receiving events
	defer receiver.Stop()

	for {
		select {
		case event := <-receiver.Events:
			if event.Event == "playbook_on_stats" {
				// convert to StatusJobEvent; would love a better way to do this
				data, err := json.Marshal(event)
				if err != nil {
					return nil, err
				}
				s := eventapi.StatusJobEvent{}
				err = json.Unmarshal(data, &s)
				if err != nil {
					return nil, err
				}
				return &s, nil
			}
		case err = <-errChan:
			// http.Server returns this in the case of being closed cleanly
			if err == http.ErrServerClosed {
				continue
			}
			if err != nil {
				return nil, err
			}
		}
	}
}
