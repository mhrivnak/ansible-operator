package eventapi

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

// EventReceiver serves the event API
type EventReceiver struct {
	// Events is the channel used by the event API handler to send JobEvents
	// back to the runner, or whatever code is using this receiver.
	Events chan JobEvent

	// server is the http.Server instance that serves the event API. It must be
	// closed.
	server io.Closer

	// urlPath is the path portion of the url at which events should be
	// received. For example, "/events/"
	urlPath string

	// stopped indicates if this receiver has permanently stopped receiving
	// events. When true, requests to POST an event will receive a "410 Gone"
	// response, and the body will be ignored.
	stopped bool

	// mutex controls access to the "stopped" bool above, ensuring that writes
	// are goroutine-safe.
	mutex sync.RWMutex
}

func New(fsPath, urlPath string, errChan chan<- error) (*EventReceiver, error) {
	listener, err := net.Listen("unix", fsPath)
	if err != nil {
		return nil, err
	}

	rec := EventReceiver{
		Events:  make(chan JobEvent, 1000),
		urlPath: urlPath,
	}

	mux := http.NewServeMux()
	mux.HandleFunc(urlPath, rec.handleEvents)
	srv := http.Server{Handler: mux}
	rec.server = &srv

	go func() {
		errChan <- srv.Serve(listener)
	}()
	return &rec, nil
}

// Close ensures that appropriate resources are cleaned up, such as any unix
// streaming socket that may be in use. Close must be called.
func (e *EventReceiver) Close() {
	e.server.Close()
}

func (e *EventReceiver) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != e.urlPath {
		http.NotFound(w, r)
		logrus.Infof("event API 404 for %s\n", r.URL.Path)
		return
	}

	if r.Method != http.MethodPost {
		logrus.Info("event API 405 wrong method")
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	e.mutex.RLock()
	if e.stopped {
		e.mutex.RUnlock()
		w.WriteHeader(http.StatusGone)
		logrus.Info("event API 410 stopped and not accepting additional events for this job")
		return
	}
	e.mutex.RUnlock()

	ct := r.Header.Get("content-type")
	if strings.Split(ct, ";")[0] != "application/json" {
		logrus.Info("event API 415 wrong content type")
		w.WriteHeader(http.StatusUnsupportedMediaType)
		w.Write([]byte("The content-type must be \"application/json\""))
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		logrus.Errorf("event API 500: %s", err.Error())
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	event := JobEvent{}
	err = json.Unmarshal(body, &event)
	if err != nil {
		logrus.Infof("event API 400, could not deserialize body: %s", err.Error())
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("Could not deserialize body as JSON"))
		return
	}

	// ansible-runner sends "status events" and "ansible events". The "status
	// events" signify a change in the state of ansible-runner itself, which
	// we're not currently interested in.
	// https://ansible-runner.readthedocs.io/en/latest/external_interface.html#event-structure
	if event.UUID == "" {
		logrus.Info("dropping event that is not a JobEvent")
	} else {
		// timeout if the channel blocks for too long
		timeout := time.NewTimer(10 * time.Second)
		select {
		case e.Events <- event:
		case <-timeout.C:
			logrus.Warn("event API 500: timed out writing event to channel")
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = timeout.Stop()
	}
	w.WriteHeader(http.StatusNoContent)
}

// Stop stops receipt of new events and starts a goroutine that consumes
// buffered events. This is useful in case the consumer of events stops
// ungracefully.
func (e *EventReceiver) Stop() {
	e.mutex.Lock()
	e.stopped = true
	e.mutex.Unlock()
	logrus.Debug("event API stopped")

	// drain Events until empty or timeout is reached
	go func() {
		timeout := time.After(5 * time.Second)
		for len(e.Events) > 0 {
			select {
			case <-e.Events:
			case <-timeout:
				logrus.Infof("event API timeout reached draining events, %d remain", len(e.Events))
				return
			}
		}
	}()
}
