// Copyright 2016 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package etcd

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"

	"github.com/go-kit/kit/log"
	"github.com/kylelemons/godebug/pretty"
	"github.com/prometheus/alertmanager/store"
	"github.com/prometheus/alertmanager/types"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

var (
	t0 = time.Now()
	t1 = t0.Add(100 * time.Millisecond)

	fakeAlertCounter = 0
	alert1           = fakeAlert()
	alert2           = fakeAlert()
	alert3           = fakeAlert()

	etcdEndpoints   = []string{"localhost:2379"}
	etcdDialTimeout = 5 * time.Second
	etcdPrefix      = "am/test/alerts-"
	alertGcInterval = 200 * time.Millisecond
)

func init() {
	pretty.CompareConfig.IncludeUnexported = true
	etcdReset()
}

// TestAlertsSubscribePutStarvation tests starvation of `iterator.Close` and
// `alerts.Put`. Both `Subscribe` and `Put` use the Alerts.mtx lock. `Subscribe`
// needs it to subscribe and more importantly unsubscribe `Alerts.listeners`. `Put`
// uses the lock to add additional alerts and iterate the `Alerts.listeners` map.
// If the channel of a listener is at its limit, `alerts.Lock` is blocked, whereby
// a listener can not unsubscribe as the lock is hold by `alerts.Lock`.
func TestAlertsSubscribePutStarvation(t *testing.T) {
	marker := types.NewMarker(prometheus.NewRegistry())
	alerts, err := NewAlerts(context.Background(), marker, 30*time.Minute, log.NewNopLogger(),
		etcdEndpoints, etcdPrefix)
	if err != nil {
		t.Fatal(err)
	}

	iterator := alerts.Subscribe()

	alertsToInsert := []*types.Alert{}
	// Exhaust alert channel
	for i := 0; i < alertChannelLength+1; i++ {
		alertsToInsert = append(alertsToInsert, &types.Alert{
			Alert: model.Alert{
				// Make sure the fingerprints differ
				Labels:       model.LabelSet{"iteration": model.LabelValue(strconv.Itoa(i))},
				Annotations:  model.LabelSet{"foo": "bar"},
				StartsAt:     t0,
				EndsAt:       t1,
				GeneratorURL: "http://example.com/prometheus",
			},
			UpdatedAt: t0,
			Timeout:   false,
		})
	}

	putIsDone := make(chan struct{})
	putsErr := make(chan error, 1)
	go func() {
		if err := alerts.Put(alertsToInsert...); err != nil {
			putsErr <- err
			return
		}

		putIsDone <- struct{}{}
	}()

	// Increase probability that `iterator.Close` is called after `alerts.Put`.
	time.Sleep(100 * time.Millisecond)
	iterator.Close()

	select {
	case <-putsErr:
		t.Fatal(err)
	case <-putIsDone:
		// continue
	case <-time.After(100 * time.Millisecond):
		t.Fatal("expected `alerts.Put` and `iterator.Close` not to starve each other")
	}
}

func TestAlertsPut(t *testing.T) {
	marker := types.NewMarker(prometheus.NewRegistry())
	alerts, err := NewAlerts(context.Background(), marker, 30*time.Minute, log.NewNopLogger(),
		etcdEndpoints, etcdPrefix)
	if err != nil {
		t.Fatal(err)
	}

	insert := []*types.Alert{alert1, alert2, alert3}

	if err := alerts.Put(insert...); err != nil {
		t.Fatalf("Insert failed: %s", err)
	}

	for i, a := range insert {
		res, err := alerts.Get(a.Fingerprint())
		if err != nil {
			t.Fatalf("retrieval error: %s", err)
		}
		if !alertsEqual(res, a) {
			t.Errorf("Unexpected alert: %d", i)
			t.Fatalf(pretty.Compare(res, a))
		}
	}
}

func TestAlertsSubscribe(t *testing.T) {
	marker := types.NewMarker(prometheus.NewRegistry())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	alerts, err := NewAlerts(ctx, marker, 30*time.Minute, log.NewNopLogger(),
		etcdEndpoints, etcdPrefix)
	if err != nil {
		t.Fatal(err)
	}

	// Add alert1 to validate if pending alerts will be sent.
	if err := alerts.Put(alert1); err != nil {
		t.Fatalf("Insert failed: %s", err)
	}

	expectedAlerts := map[model.Fingerprint]*types.Alert{
		alert1.Fingerprint(): alert1,
		alert2.Fingerprint(): alert2,
		alert3.Fingerprint(): alert3,
	}

	// Start many consumers and make sure that each receives all the subsequent alerts.
	var (
		nb     = 100
		fatalc = make(chan string, nb)
		wg     sync.WaitGroup
	)
	wg.Add(nb)
	for i := 0; i < nb; i++ {
		go func(i int) {
			defer wg.Done()

			it := alerts.Subscribe()
			defer it.Close()

			received := make(map[model.Fingerprint]struct{})
			for {
				select {
				case got, ok := <-it.Next():
					if !ok {
						fatalc <- fmt.Sprintf("Iterator %d closed", i)
						return
					}
					if it.Err() != nil {
						fatalc <- fmt.Sprintf("Iterator %d: %v", i, it.Err())
						return
					}
					expected := expectedAlerts[got.Fingerprint()]
					if !alertsEqual(got, expected) {
						fatalc <- fmt.Sprintf("Unexpected alert (iterator %d)\n%s", i, pretty.Compare(got, expected))
						return
					}
					received[got.Fingerprint()] = struct{}{}
					if len(received) == len(expectedAlerts) {
						return
					}
				case <-time.After(5 * time.Second):
					fatalc <- fmt.Sprintf("Unexpected number of alerts for iterator %d, got: %d, expected: %d", i, len(received), len(expectedAlerts))
					return
				}
			}
		}(i)
	}

	// Add more alerts that should be received by the subscribers.
	if err := alerts.Put(alert2); err != nil {
		t.Fatalf("Insert failed: %s", err)
	}
	if err := alerts.Put(alert3); err != nil {
		t.Fatalf("Insert failed: %s", err)
	}

	wg.Wait()
	close(fatalc)
	fatal, ok := <-fatalc
	if ok {
		t.Fatalf(fatal)
	}
}

func TestAlertsGetPending(t *testing.T) {
	marker := types.NewMarker(prometheus.NewRegistry())
	alerts, err := NewAlerts(context.Background(), marker, 30*time.Minute, log.NewNopLogger(),
		etcdEndpoints, etcdPrefix)
	if err != nil {
		t.Fatal(err)
	}

	if err := alerts.Put(alert1, alert2); err != nil {
		t.Fatalf("Insert failed: %s", err)
	}

	expectedAlerts := map[model.Fingerprint]*types.Alert{
		alert1.Fingerprint(): alert1,
		alert2.Fingerprint(): alert2,
	}
	iterator := alerts.GetPending()
	for actual := range iterator.Next() {
		expected := expectedAlerts[actual.Fingerprint()]
		if !alertsEqual(actual, expected) {
			t.Errorf("Unexpected alert")
			t.Fatalf(pretty.Compare(actual, expected))
		}
	}

	if err := alerts.Put(alert3); err != nil {
		t.Fatalf("Insert failed: %s", err)
	}

	expectedAlerts = map[model.Fingerprint]*types.Alert{
		alert1.Fingerprint(): alert1,
		alert2.Fingerprint(): alert2,
		alert3.Fingerprint(): alert3,
	}
	iterator = alerts.GetPending()
	for actual := range iterator.Next() {
		expected := expectedAlerts[actual.Fingerprint()]
		if !alertsEqual(actual, expected) {
			t.Errorf("Unexpected alert")
			t.Fatalf(pretty.Compare(actual, expected))
		}
	}
}

func TestAlertsGC(t *testing.T) {
	marker := types.NewMarker(prometheus.NewRegistry())
	alerts, err := NewAlerts(context.Background(), marker, alertGcInterval, log.NewNopLogger(),
		etcdEndpoints, etcdPrefix)
	if err != nil {
		t.Fatal(err)
	}

	insert := []*types.Alert{alert1, alert2, alert3}

	if err := alerts.Put(insert...); err != nil {
		t.Fatalf("Insert failed: %s", err)
	}

	for _, a := range insert {
		marker.SetActive(a.Fingerprint())
		if !marker.Active(a.Fingerprint()) {
			t.Errorf("error setting status: %v", a)
		}
	}

	time.Sleep(300 * time.Millisecond)

	for i, a := range insert {
		_, err := alerts.Get(a.Fingerprint())
		require.Error(t, err)
		require.Equal(t, store.ErrNotFound, err, fmt.Sprintf("alert %d didn't get GC'd: %v", i, err))

		s := marker.Status(a.Fingerprint())
		if s.State != types.AlertStateUnprocessed {
			t.Errorf("marker %d didn't get GC'd: %v", i, s)
		}
	}
}

func alertsEqual(a1, a2 *types.Alert) bool {
	if a1 == nil || a2 == nil {
		return false
	}
	if !reflect.DeepEqual(a1.Labels, a2.Labels) {
		return false
	}
	if !reflect.DeepEqual(a1.Annotations, a2.Annotations) {
		return false
	}
	if a1.GeneratorURL != a2.GeneratorURL {
		return false
	}
	if !a1.StartsAt.Equal(a2.StartsAt) {
		return false
	}
	if !a1.EndsAt.Equal(a2.EndsAt) {
		return false
	}
	if !a1.UpdatedAt.Equal(a2.UpdatedAt) {
		return false
	}
	return a1.Timeout == a2.Timeout
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
	if err := alerts.etcdPut(a1); err != nil {
		t.Errorf("etcdPut failed: %s", err)
	}
	a2, err := alerts.etcdGet(a1.Fingerprint())
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
	if str1, err = etcdMarshallAlert(a1); err != nil {
		t.Errorf("marshall alert failed: %s", err)
	}
	if a2, err = etcdUnmarshallAlert(str1); err != nil {
		t.Errorf("unmarshall alert failed: %s", err)
	}
	if str2, err = etcdMarshallAlert(a2); err != nil {
		t.Errorf("re-marshall alert failed: %s", err)
	}
	if str1 != str2 {
		t.Error("alert string comparison failed")
	}
	if !alertsEqual(a1, a2) {
		t.Error("alert struct comparison failed")
	}
}

func TestEtcdWatch(t *testing.T) {
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

	alerts.EtcdRunWatch(context.Background())
	iterator := alerts.Subscribe()
	time.Sleep(100 * time.Millisecond) // allow the subscribe time to kick in

	// send all of the alerts
	alertsToSend := []*types.Alert{fakeAlert(), fakeAlert(), fakeAlert()}
	for _, a := range alertsToSend {
		if err := alerts.etcdPut(a); err != nil {
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
