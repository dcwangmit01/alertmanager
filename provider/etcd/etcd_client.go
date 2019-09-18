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

/* HOW IT WORKS

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

	"github.com/prometheus/alertmanager/types"
)

var (
	ErrorEtcdNotInitialized = errors.New("Etcd not initialized")
)

type EtcdClient struct {
	alerts    *Alerts
	endpoints []string
	prefix    string
	logger    log.Logger
	client    *clientv3.Client
	mtx       sync.Mutex
}

func NewEtcdClient(ctx context.Context, a *Alerts, endpoints []string, prefix string) (*EtcdClient, error) {

	ec := &EtcdClient{
		alerts:    a,
		endpoints: endpoints,
		prefix:    prefix,
		logger:    log.With(a.logger, "component", "provider.etcd"),
	}

	// create the configuration
	etcdConfig := clientv3.Config{
		Endpoints:        endpoints,
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
		level.Error(ec.logger).Log("msg", "Etcd connection failed", "err", err)
		os.Exit(1)
	} else {
		level.Info(ec.logger).Log("msg", "Etcd connection successful")
	}
	ec.mtx.Lock()
	ec.client = client
	ec.mtx.Unlock()

	// start a goroutine to ensure the client will be cleaned up when the context is done
	go func() {
		defer func() {
			ec.mtx.Lock()
			if ec.client != nil {
				ec.client.Close()
				ec.client = nil
			}
			ec.mtx.Unlock()
			level.Info(ec.logger).Log("msg", "Etcd connection shut down")
		}()

		for {
			select {
			case <-ctx.Done():
				return
			}
		}
	}()
	return ec, nil
}

func (ec *EtcdClient) CheckAndPut(alert *types.Alert) error {
	// Reduce writes to Etcd.  Only put to Etcd if the current alert is different than the same
	// alert in etcd (excluding/ignoring the alert.UpdatedAt field).  To do this we:
	//   1) Fetch the alert with the same fingerprint from Etcd
	//   2) Compare our current alert with the alert from Etcd.
	//   3) If the alerts are different, then write it back.
	//
	// This mechanism is simple and convergent. It is more effiencient to get from Etcd, check,
	// then put into Etcd because unnecessarily putting any alert into etcd will result in the
	// put alert being sent to all the AMs which are watching etcd.

	etcdAlert, err := ec.Get(alert.Fingerprint())
	if err == nil {
		if AlertsEqualExceptForUpdatedAt(etcdAlert, alert) {
			return nil // skip write to etcd
		}
	}

	return ec.Put(alert)
}

func (ec *EtcdClient) Put(alert *types.Alert) error {
	// We do a best effort.  If etcd is not initialized yet, then skip
	if ec.client == nil {
		level.Error(ec.logger).Log("msg", "Not putting alert to etcd, etcd not initialized yet")
		return ErrorEtcdNotInitialized
	}

	fp := alert.Fingerprint()
	alertStr, err := MarshallAlert(alert)
	if err != nil {
		level.Error(ec.logger).Log("msg", "Error marshalling JSON Alert", "err", err)
		return err
	}

	ec.mtx.Lock()
	_, err = ec.client.Put(context.TODO(), ec.prefix+fp.String(), alertStr)
	ec.mtx.Unlock()
	if err != nil {
		level.Error(ec.logger).Log("msg", "Error putting alert to etcd", "err", err)
		return err
	}
	return nil
}

func (ec *EtcdClient) Get(fp model.Fingerprint) (*types.Alert, error) {
	// We do a best effort.  If etcd is not initialized yet, then skip
	if ec.client == nil {
		level.Error(ec.logger).Log("msg", "Not getting alert from etcd, etcd not initialized yet")
		return nil, ErrorEtcdNotInitialized
	}

	// ensure the get operation does not take too long
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	ec.mtx.Lock()
	resp, err := ec.client.Get(ctx, ec.prefix+fp.String())
	ec.mtx.Unlock()
	if err != nil {
		level.Error(ec.logger).Log("msg", "Error getting alert from etcd", "err", err)
		return nil, err
	}

	if len(resp.Kvs) != 1 {
		return nil, errors.New("etcdGet did not get exactly one result for fingerprint")
	}

	alert, err := UnmarshallAlert(string(resp.Kvs[0].Value))
	if err != nil {
		level.Error(ec.logger).Log("msg", "Error unmarshalling JSON Alert", "err", err)
		return nil, err
	}

	return alert, nil
}

func (ec *EtcdClient) RunWatch(ctx context.Context) {
	// watch for alert changes in etcd and writes them back to our
	// local alert state
	ctx = clientv3.WithRequireLeader(ctx)

	go func() {

		ec.mtx.Lock()
		rch := ec.client.Watch(ctx, ec.prefix, clientv3.WithPrefix())
		ec.mtx.Unlock()

		for wresp := range rch {
			for _, ev := range wresp.Events {
				level.Debug(ec.logger).Log("msg", "watch received",
					"type", ev.Type, "key", fmt.Sprintf("%q", ev.Kv.Key), "value", fmt.Sprintf("%q", ev.Kv.Value))
				alert, err := UnmarshallAlert(string(ev.Kv.Value))
				if err != nil {
					continue
				}
				ec.alerts.Put(alert)
			}
		}
	}()
}

func (ec *EtcdClient) RunLoadAllAlerts(ctx context.Context) {
	go func() {
		for {
			ec.mtx.Lock()
			resp, err := ec.client.Get(ctx, ec.prefix, clientv3.WithPrefix())
			ec.mtx.Unlock()
			if err != nil {
				level.Error(ec.logger).Log("msg", "Error fetching all alerts etcd", "err", err)
				time.Sleep(5 * time.Second)
				continue
			}

			for _, ev := range resp.Kvs {
				level.Debug(ec.logger).Log("msg", "get received",
					"key", fmt.Sprintf("%q", ev.Key), "value", fmt.Sprintf("%q", ev.Value))
				alert, err := UnmarshallAlert(string(ev.Value))
				if err != nil {
					continue
				}
				ec.alerts.Put(alert)
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

func MarshallAlert(alert *types.Alert) (string, error) {
	b, err := json.Marshal(alert)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func UnmarshallAlert(alertStr string) (*types.Alert, error) {
	var alert types.Alert
	err := json.Unmarshal([]byte(alertStr), &alert)
	if err != nil {
		return nil, err
	}
	return &alert, nil
}
