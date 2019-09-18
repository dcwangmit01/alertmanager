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
	"errors"
	"fmt"
	"os"
	"reflect"
	"sync"
	"time"

	"github.com/coreos/etcd/clientv3"
	"google.golang.org/grpc"

	"github.com/go-kit/kit/log"
	"github.com/go-kit/kit/log/level"
	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/provider"
	"github.com/prometheus/alertmanager/store"
	"github.com/prometheus/alertmanager/types"
)

const alertChannelLength = 200

// Alerts gives access to a set of alerts. All methods are goroutine-safe.
type Alerts struct {
	cancel context.CancelFunc

	mtx       sync.Mutex
	alerts    *store.Alerts
	listeners map[int]listeningAlerts
	next      int

	logger log.Logger

	etcdMtx       sync.Mutex
	etcdClient    *clientv3.Client
	etcdPrefix    string
	etcdEndpoints []string
}

type listeningAlerts struct {
	alerts chan *types.Alert
	done   chan struct{}
}

// NewAlerts returns a new alert provider.
func NewAlerts(ctx context.Context, m types.Marker, intervalGC time.Duration, l log.Logger, etcdEndpoints []string, etcdPrefix string) (*Alerts, error) {
	ctx, cancel := context.WithCancel(ctx)
	a := &Alerts{
		alerts:    store.NewAlerts(intervalGC),
		cancel:    cancel,
		listeners: map[int]listeningAlerts{},
		next:      0,
		logger:    log.With(l, "component", "provider"),
	}
	a.alerts.SetGCCallback(func(alerts []*types.Alert) {
		for _, alert := range alerts {
			// As we don't persist alerts, we no longer consider them after
			// they are resolved. Alerts waiting for resolved notifications are
			// held in memory in aggregation groups redundantly.
			m.Delete(alert.Fingerprint())
		}

		a.mtx.Lock()
		for i, l := range a.listeners {
			select {
			case <-l.done:
				delete(a.listeners, i)
				close(l.alerts)
			default:
				// listener is not closed yet, hence proceed.
			}
		}
		a.mtx.Unlock()
	})
	a.alerts.Run(ctx)

	// initialize etcd client and run loops
	a.etcdPrefix = etcdPrefix
	a.etcdEndpoints = etcdEndpoints
	a.etcdRunClient(ctx)

	return a, nil
}

// Close the alert provider.
func (a *Alerts) Close() {
	if a.cancel != nil {
		a.cancel()
	}
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// Subscribe returns an iterator over active alerts that have not been
// resolved and successfully notified about.
// They are not guaranteed to be in chronological order.
func (a *Alerts) Subscribe() provider.AlertIterator {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	var (
		done   = make(chan struct{})
		alerts = a.alerts.List()
		ch     = make(chan *types.Alert, max(len(alerts), alertChannelLength))
	)

	for _, a := range alerts {
		ch <- a
	}

	a.listeners[a.next] = listeningAlerts{alerts: ch, done: done}
	a.next++

	return provider.NewAlertIterator(ch, done, nil)
}

// GetPending returns an iterator over all the alerts that have
// pending notifications.
func (a *Alerts) GetPending() provider.AlertIterator {
	var (
		ch   = make(chan *types.Alert, alertChannelLength)
		done = make(chan struct{})
	)

	go func() {
		defer close(ch)

		for _, a := range a.alerts.List() {
			select {
			case ch <- a:
			case <-done:
				return
			}
		}
	}()

	return provider.NewAlertIterator(ch, done, nil)
}

// Get returns the alert for a given fingerprint.
func (a *Alerts) Get(fp model.Fingerprint) (*types.Alert, error) {
	return a.alerts.Get(fp)
}

// Put adds the given alert to the set.
func (a *Alerts) Put(alerts ...*types.Alert) error {

	for _, alert := range alerts {
		fp := alert.Fingerprint()

		// Check that there's an alert existing within the store before
		// trying to merge.
		if old, err := a.alerts.Get(fp); err == nil {
			// Merge alerts if there is an overlap in activity range.
			if (alert.EndsAt.After(old.StartsAt) && alert.EndsAt.Before(old.EndsAt)) ||
				(alert.StartsAt.After(old.StartsAt) && alert.StartsAt.Before(old.EndsAt)) {
				alert = old.Merge(alert)
			}
		}

		if err := a.alerts.Set(alert); err != nil {
			level.Error(a.logger).Log("msg", "error on set alert", "err", err)
			continue
		}
		if err := a.etcdCheckAndPut(alert); err != nil {
			level.Error(a.logger).Log("msg", "error on etcdPut alert", "err", err)
			continue
		}

		a.mtx.Lock()
		for _, l := range a.listeners {
			select {
			case l.alerts <- alert:
			case <-l.done:
			}
		}
		a.mtx.Unlock()
	}

	return nil
}

func (a *Alerts) etcdRunClient(ctx context.Context) {

	// create the configuration
	etcdConfig := clientv3.Config{
		Endpoints:        a.etcdEndpoints,
		AutoSyncInterval: 60 * time.Second,
		DialTimeout:      10 * time.Second,
		DialOptions:      []grpc.DialOption{grpc.WithBlock()}, // block until connect
	}

	// create the client
	client, err := clientv3.New(etcdConfig)
	if err != nil {
		// On startup, if we cannot connect to the etcd cluster, then fail hard so that the
		// user may address a potential configuration issue.  Once the clientv3 connects
		// successfully, clientv3 will reconnect to the etcd cluster as it goes down or up,
		// or into or out of network connectivity.
		level.Error(a.logger).Log("msg", "Etcd connection failed", "err", err)
		os.Exit(1)
	} else {
		level.Info(a.logger).Log("msg", "Etcd connection successful")
	}
	a.etcdMtx.Lock()
	a.etcdClient = client
	a.etcdMtx.Unlock()

	// start a goroutine to ensure the client will be cleaned up when the context is done
	go func() {
		defer func() {
			a.etcdMtx.Lock()
			if a.etcdClient != nil {
				a.etcdClient.Close()
				a.etcdClient = nil
			}
			a.etcdMtx.Unlock()
			level.Info(a.logger).Log("msg", "Etcd connection shut down")
		}()

		for {
			select {
			case <-ctx.Done():
				return
			}
		}
	}()

}

var (
	ErrorEtcdNotInitialized = errors.New("Etcd not initialized")
)

func etcdMarshallAlert(alert *types.Alert) (string, error) {
	b, err := json.Marshal(alert)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func etcdUnmarshallAlert(alertStr string) (*types.Alert, error) {
	var alert types.Alert
	err := json.Unmarshal([]byte(alertStr), &alert)
	if err != nil {
		return nil, err
	}
	return &alert, nil
}

func (a *Alerts) etcdCheckAndPut(alert *types.Alert) error {
	// Reduce writes to Etcd.  Only put to Etcd if the current alert is different than the same
	// alert in etcd (excluding/ignoring the alert.UpdatedAt field).  To do this we:
	//   1) Fetch the alert with the same fingerprint from Etcd
	//   2) Compare our current alert with the alert from Etcd.
	//   3) If the alerts are different, then write it back.
	//
	// This mechanism is simple and convergent. It is more effiencient to get from Etcd, check,
	// then put into Etcd because unnecessarily putting any alert into etcd will result in the
	// put alert being sent to all the AMs which are watching etcd.

	etcdAlert, err := a.etcdGet(alert.Fingerprint())
	if err == nil {
		level.Debug(a.logger).Log("msg", "etcdCheckAndPut found alert already exists in etcd")
		if AlertsEqualExceptForUpdatedAt(etcdAlert, alert) {
			level.Debug(a.logger).Log("msg", "etcdCheckAndPut found alert equals that found in etcd")
			return nil
		}
	}

	return a.etcdPut(alert)
}

func (a *Alerts) etcdPut(alert *types.Alert) error {
	// We do a best effort.  If etcd is not initialized yet, then skip
	if a.etcdClient == nil {
		level.Error(a.logger).Log("msg", "Not putting alert to etcd, etcd not initialized yet")
		return ErrorEtcdNotInitialized
	}

	fp := alert.Fingerprint()
	alertStr, err := etcdMarshallAlert(alert)
	if err != nil {
		level.Error(a.logger).Log("msg", "Error marshalling JSON Alert", "err", err)
		return err
	}

	a.etcdMtx.Lock()
	_, err = a.etcdClient.Put(context.TODO(), a.etcdPrefix+fp.String(), alertStr)
	a.etcdMtx.Unlock()
	if err != nil {
		level.Error(a.logger).Log("msg", "Error putting alert to etcd", "err", err)
		return err
	}
	return nil
}

func (a *Alerts) etcdGet(fp model.Fingerprint) (*types.Alert, error) {
	// We do a best effort.  If etcd is not initialized yet, then skip
	if a.etcdClient == nil {
		level.Error(a.logger).Log("msg", "Not getting alert from etcd, etcd not initialized yet")
		return nil, ErrorEtcdNotInitialized
	}

	// ensure the get operation does not take too long
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	a.etcdMtx.Lock()
	resp, err := a.etcdClient.Get(ctx, a.etcdPrefix+fp.String())
	a.etcdMtx.Unlock()
	if err != nil {
		level.Error(a.logger).Log("msg", "Error getting alert from etcd", "err", err)
		return nil, err
	}

	if len(resp.Kvs) != 1 {
		return nil, errors.New("etcdGet did not get exactly one result for fingerprint")
	}

	alert, err := etcdUnmarshallAlert(string(resp.Kvs[0].Value))
	if err != nil {
		level.Error(a.logger).Log("msg", "Error unmarshalling JSON Alert", "err", err)
		return nil, err
	}

	return alert, nil
}

func (a *Alerts) EtcdRunWatch(ctx context.Context) {
	// watch for alert changes in etcd and writes them back to our
	// local alert state
	ctx = clientv3.WithRequireLeader(ctx)

	go func() {

		a.etcdMtx.Lock()
		rch := a.etcdClient.Watch(ctx, a.etcdPrefix, clientv3.WithPrefix())
		a.etcdMtx.Unlock()

		for wresp := range rch {
			for _, ev := range wresp.Events {
				level.Debug(a.logger).Log("msg", "watch received",
					"type", ev.Type, "key", fmt.Sprintf("%q", ev.Kv.Key), "value", fmt.Sprintf("%q", ev.Kv.Value))
				alert, err := etcdUnmarshallAlert(string(ev.Kv.Value))
				if err != nil {
					continue
				}
				a.Put(alert)
			}
		}
	}()
}

func (a *Alerts) EtcdRunLoadAllAlerts(ctx context.Context) {
	go func() {
		for {
			a.etcdMtx.Lock()
			resp, err := a.etcdClient.Get(ctx, a.etcdPrefix, clientv3.WithPrefix())
			a.etcdMtx.Unlock()
			if err != nil {
				level.Error(a.logger).Log("msg", "Error fetching all alerts etcd", "err", err)
				time.Sleep(5 * time.Second)
				continue
			}

			for _, ev := range resp.Kvs {
				level.Debug(a.logger).Log("msg", "get received",
					"key", fmt.Sprintf("%q", ev.Key), "value", fmt.Sprintf("%q", ev.Value))
				alert, err := etcdUnmarshallAlert(string(ev.Value))
				if err != nil {
					continue
				}
				a.Put(alert)
			}
			break
		}
	}()
}

// Equals returns a true if the two alerts have the same values, false otherwise
func AlertsEqualExceptForUpdatedAt(a *types.Alert, o *types.Alert) bool {
	if a == nil || o == nil {
		return false
	}
	if !reflect.DeepEqual(a.Labels, o.Labels) {
		return false
	}
	if !reflect.DeepEqual(a.Annotations, o.Annotations) {
		return false
	}
	if a.GeneratorURL != o.GeneratorURL {
		return false
	}
	if !a.StartsAt.Equal(o.StartsAt) {
		return false
	}
	if !a.EndsAt.Equal(o.EndsAt) {
		return false
	}
	// we explicitly ignore UpdatedAt
	/*
		if !a.UpdatedAt.Equal(o.UpdatedAt) {
			return false
		}
	*/
	return a.Timeout == o.Timeout
}

/*
NOTES
https://github.com/etcd-io/etcd/blob/master/clientv3/example_test.go
https://godoc.org/github.com/coreos/etcd/clientv3#WithRequireLeader
https://github.com/etcd-io/etcd/blob/master/clientv3/integration/watch_test.go

Sequence of Events

Initialization (Synchronous)

* On startup, AM tries to connect to Etcd.  If the connection attempt is unsuccessful after a
  timeout, then AM fails hard because it's probably a configuration error (Kubernetes or some other
  supervisor will restart it).  Once AM successfully connects to Etcd, it is from then on a "best
  effort" to write alerts to Etcd as well receive alert updates from the Etcd watch.  AM continues
  running independently to maintain availability, and then will resume writing to Etcd once the
  connection is re-established.
  * The Etcd clientv3 library will automatically reconnect if the Etcd server goes down or up, or
    upon resuming network connectivity if it had been lost.
* Subscribe to Etcd Watch Events, which are the sends alert updates through the Put method.
  * The long running watch subscription will reconnect if the watch channel is closed.
* Proceed to load all alerts from Etcd.

Ongoing Operation (Async)
* AM can receive alerts from both API requests AND Etcd watches

*/
