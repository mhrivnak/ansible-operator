package leader

import (
	"errors"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"github.com/operator-framework/operator-sdk/pkg/k8sclient"
	"github.com/operator-framework/operator-sdk/pkg/sdk"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/sirupsen/logrus"
)

var ErrNoNS = errors.New("namespace not found for current environment")

func TryBecome(name string) error {
	err := Become(name)
	if err == ErrNoNS {
		logrus.Warn("leader election disabled; no namespace found for current environment")
		return nil
	}
	return err
}

// Become ensures that the current pod is the leader within its namespace. It
// continuously tries to create a ConfigMap with an agreed-upon name and the
// current pod set as the owner reference. Only one can exist at a time with
// the same name, so the pod that successfully creates the ConfigMap is the
// leader. Upon termination of that pod, the garbage collector will delete the
// ConfigMap, enabling a different pod to become the leader.
func Become(name string) error {
	ns, err := myNS()
	if err != nil {
		return err
	}
	owner, err := myOwnerRef(ns)
	if err != nil {
		return err
	}

	cm := &corev1.ConfigMap{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "ConfigMap",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       ns,
			OwnerReferences: []metav1.OwnerReference{owner},
		},
	}

	// check for existing lock from this pod, in case we got restarted
	cmClient, _, err := k8sclient.GetResourceClient("v1", "ConfigMap", ns)
	if err != nil {
		logrus.Error("failed to get configmap client")
		return err
	}
	existing, err := cmClient.Get(name, metav1.GetOptions{})
	switch {
	case err == nil:
		for _, existingOwner := range existing.GetOwnerReferences() {
			if existingOwner.Name == owner.Name {
				logrus.Info("Found existing lock with my name. I was likely restarted.")
				logrus.Info("Continuing as the leader.")
				return nil
			} else {
				logrus.Infof("Found existing lock from %s", existingOwner.Name)
			}
		}
	case apierrors.IsNotFound(err):
		logrus.Info("No pre-existing lock was found.")
	default:
		logrus.Error("unknown error trying to get ConfigMap")
		return err
	}

	// try to create a lock
	for {
		err := sdk.Create(cm)
		switch {
		case err == nil:
			logrus.Info("Became the leader.")
			return nil
		case apierrors.IsAlreadyExists(err):
			logrus.Info("Not the leader. Waiting.")
			time.Sleep(time.Second * 1)
		default:
			logrus.Error("unknown error creating configmap")
			return err
		}
	}
}

func myNS() (string, error) {
	nsBytes, err := ioutil.ReadFile("/var/run/secrets/kubernetes.io/serviceaccount/namespace")
	if err != nil {
		if os.IsNotExist(err) {
			return "", ErrNoNS
		}
		return "", err
	}
	ns := strings.TrimSpace(string(nsBytes))
	logrus.Infof("found namespace: %s", ns)
	return ns, nil
}

func myOwnerRef(ns string) (metav1.OwnerReference, error) {
	hostname, err := os.Hostname()
	if err != nil {
		return metav1.OwnerReference{}, err
	}
	logrus.Infof("found hostname: %s", hostname)

	pClient, _, err := k8sclient.GetResourceClient("v1", "Pod", ns)
	if err != nil {
		logrus.Error("failed to get pod client")
		return metav1.OwnerReference{}, err
	}

	myPod, err := pClient.Get(hostname, metav1.GetOptions{})
	if err != nil {
		logrus.Error("failed to get pod")
		return metav1.OwnerReference{}, err
	}

	owner := metav1.OwnerReference{
		APIVersion: myPod.GetAPIVersion(),
		Kind:       myPod.GetKind(),
		Name:       myPod.GetName(),
		UID:        myPod.GetUID(),
	}
	return owner, nil
}
