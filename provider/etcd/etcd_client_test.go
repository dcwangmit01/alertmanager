package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
)

var (
	fakeAlertCounter = 0

	etcdEndpoints   = []string{"localhost:2379"}
	etcdDialTimeout = 5 * time.Second
	etcdPrefix      = "am/test/alerts-"
	alertGcInterval = 200 * time.Millisecond
)

func init() {
	etcdReset()
}

func TestEtcdWriteReadAlert(t *testing.T) {
	defer etcdReset()

	marker := types.NewMarker(prometheus.NewRegistry())
	debug := true

	var logger log.Logger
	if debug {
		w := log.NewSyncWriter(os.Stderr)
		logger = log.NewJSONLogger(w)
	} else {
		logger = log.NewNopLogger()
	}

	alerts, err := NewAlerts(context.Background(), marker, alertGcInterval, logger,
		etcdEndpoints, etcdPrefix)
	if err != nil {
		t.Fatal(err)
	}

	a1 := fakeAlert()
	if err := alerts.EtcdClient.Put(a1); err != nil {
		t.Errorf("etcdPut failed: %s", err)
	}
	a2, err := alerts.EtcdClient.Get(a1.Fingerprint())
	if err != nil {
		t.Errorf("etcdGet failed: %s", err)
	}
	if !alertsEqual(a1, a2) {
		t.Error("alert struct comparison failed")
	}
}

func TestEtcdMarshallUnmarshallAlert(t *testing.T) {
	defer etcdReset()

	var str1, str2 string
	var err error
	var a1, a2 *types.Alert

	a1 = fakeAlert()
	if str1, err = MarshallAlert(a1); err != nil {
		t.Errorf("marshall alert failed: %s", err)
	}
	if a2, err = UnmarshallAlert(str1); err != nil {
		t.Errorf("unmarshall alert failed: %s", err)
	}
	if str2, err = MarshallAlert(a2); err != nil {
		t.Errorf("re-marshall alert failed: %s", err)
	}
	if str1 != str2 {
		t.Error("alert string comparison failed")
	}
	if !alertsEqual(a1, a2) {
		t.Error("alert struct comparison failed")
	}
}

func TestEtcdRunWatch(t *testing.T) {
	defer etcdReset()

	marker := types.NewMarker(prometheus.NewRegistry())
	debug := true

	var logger log.Logger
	if debug {
		w := log.NewSyncWriter(os.Stderr)
		logger = log.NewJSONLogger(w)
	} else {
		logger = log.NewNopLogger()
	}

	alerts, err := NewAlerts(context.Background(), marker, alertGcInterval, logger,
		etcdEndpoints, etcdPrefix)
	if err != nil {
		t.Fatal(err)
	}

	alerts.EtcdClient.RunWatch(context.Background())
	iterator := alerts.Subscribe()
	time.Sleep(100 * time.Millisecond) // allow the subscribe time to kick in

	// send all of the alerts
	alertsToSend := []*types.Alert{fakeAlert(), fakeAlert(), fakeAlert()}
	for _, a := range alertsToSend {
		if err := alerts.EtcdClient.Put(a); err != nil {
			t.Errorf("etcdPut failed: %s", err)
		}
	}

	// read the alerts back in order
	index := 0
	for alert := range iterator.Next() {
		if !alertsEqual(alert, alertsToSend[index]) {
			t.Error("alert struct comparison failed")
		}
		index += 1
		if index == len(alertsToSend) {
			break
		}
	}
}

func etcdReset() {
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   etcdEndpoints,
		DialTimeout: etcdDialTimeout,
	})
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
	defer cli.Close()

	// delete the keys
	_, err = cli.Delete(context.Background(), etcdPrefix, clientv3.WithPrefix())
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}
}

func fakeAlert() *types.Alert {
	fakeAlertCounter += 1

	labelSetJSON := fmt.Sprintf(`{ "labelSet": {
		"foo%d": "bar%d",
                "time": "%s"
	}}`, fakeAlertCounter, fakeAlertCounter, time.Now().String())

	type testConfig struct {
		LabelSet model.LabelSet `yaml:"labelSet,omitempty"`
	}

	var c testConfig
	err := json.Unmarshal([]byte(labelSetJSON), &c)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(1)
	}

	a := &types.Alert{
		Alert: model.Alert{
			Labels:       c.LabelSet,
			Annotations:  model.LabelSet{"foo": "bar"},
			StartsAt:     t0,
			EndsAt:       t1,
			GeneratorURL: "http://example.com/prometheus",
		},
		UpdatedAt: t0,
		Timeout:   false,
	}
	return a
}
