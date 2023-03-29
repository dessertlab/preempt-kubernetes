/*
Copyright 2015 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package workqueue

import (
	"sync"
	"time"

	"k8s.io/utils/clock"
)

type Interface interface {
	Add(item interface{}, priority ...int)
	ValidateorAdd(item interface{}, priority int)
	AddInvalid(item interface{}, priority int)
	Len() int
	Get() (item interface{}, shutdown bool)
	GetDeterministic() (item interface{}, code int8)
	Done(item interface{}, priority ...int)
	ShutDown()
	ShutDownWithDrain()
	ShuttingDown() bool
}

// New constructs a new work queue (see the package comment).
func New() *Type {
	return NewNamed("")
}

func NewNamed(name string) *Type {
	rc := clock.RealClock{}
	return newQueue(
		rc,
		globalMetricsFactory.newQueueMetrics(name, rc),
		defaultUnfinishedWorkUpdatePeriod,
		name,
	)
}

func newQueue(c clock.WithTicker, metrics queueMetrics, updatePeriod time.Duration, name string) *Type {
	t := &Type{
		clock:                      c,
		isempty:                    true,
		dirty:                      set{},
		processing:                 set{},
		cond:                       sync.NewCond(&sync.Mutex{}),
		metrics:                    metrics,
		unfinishedWorkUpdatePeriod: updatePeriod,
		name:                       name,
	}

	// Don't start the goroutine for a type of noMetrics so we don't consume
	// resources unnecessarily
	if _, ok := metrics.(noMetrics); !ok {
		go t.updateUnfinishedWorkLoop()
	}

	return t
}

const defaultUnfinishedWorkUpdatePeriod = 500 * time.Millisecond

// Type is a work queue (see the package comment).
const CRITICALITIES = 3

type Type struct {
	// queue defines the order in which we will work on items. Every
	// element of queue should be in the dirty set and not in the
	// processing set.
	queue [CRITICALITIES][]t
	valid [CRITICALITIES][]bool

	isempty bool

	// dirty defines all of the items that need to be processed.
	dirty set

	// Things that are currently being processed are in the processing set.
	// These things may be simultaneously in the dirty set. When we finish
	// processing something and remove it from this set, we'll check if
	// it's in the dirty set, and if so, add it to the queue.
	processing set

	cond *sync.Cond

	shuttingDown bool
	drain        bool

	metrics queueMetrics

	unfinishedWorkUpdatePeriod time.Duration
	clock                      clock.WithTicker
	name                       string
}

type empty struct{}
type t interface{}
type set map[t]empty

func (s set) has(item t) bool {
	_, exists := s[item]
	return exists
}

func (s set) insert(item t) {
	s[item] = empty{}
}

func (s set) delete(item t) {
	delete(s, item)
}

func (s set) len() int {
	return len(s)
}

// Add marks item as needing processing.
func (q *Type) Add(item interface{}, priority ...int) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}
	if q.dirty.has(item) {
		return
	}

	q.metrics.add(item)

	q.dirty.insert(item)
	if q.processing.has(item) {
		return
	}

	prio := 0
	if len(priority) > 0 {
		prio = priority[0]
		if prio > CRITICALITIES-1 {
			prio = CRITICALITIES - 1
		}
	}
	q.queue[prio] = append(q.queue[prio], item)
	q.valid[prio] = append(q.valid[prio], true)

	//klog.Infof("%s GREPTAG Item added at prio signaling %d", q.name, prio)
	q.isempty = false

	q.cond.Signal()
}

// Add marks item as needing processing.
// Item is not yet really in the queue, just setting order
// No signal is called here and empty is not changed
// WARNING: SHOULD NOT BE USED TOGETHER WITH GET
// GET IGNORES THE VALID FIELD
func (q *Type) AddInvalid(item interface{}, prio int) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}
	if q.dirty.has(item) {
		return
	}

	q.metrics.add(item)

	q.dirty.insert(item)
	if q.processing.has(item) {
		return
	}

	if prio > CRITICALITIES-1 {
		prio = CRITICALITIES - 1
	}

	q.queue[prio] = append(q.queue[prio], item)
	q.valid[prio] = append(q.valid[prio], false)

	//klog.Infof("%s GREPTAG Item added at prio signaling %d", q.name, prio)
}

// Set the item as valid now and signal blocked Get, if not present Add
// empty is set to false and signal called, as if it was and Add
func (q *Type) ValidateorAdd(item interface{}, prio int) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	if q.shuttingDown {
		return
	}

	if prio > CRITICALITIES-1 {
		prio = CRITICALITIES - 1
	}

	// if element is dirty, it means either that it is in the queue and invalid
	// or that it has been added after being get, in that case we behave normally.
	// The item cannot be processing if invalid, thus only dirty condition is to be checked
	if q.dirty.has(item) {
		for i := 0; i < len(q.queue[prio]); i++ {
			itemf := q.queue[prio][i]
			if item == itemf && !q.valid[prio][i] {
				q.valid[prio][i] = true
				q.isempty = false
				q.cond.Signal()
				return
			}
		}
		return
	}

	q.metrics.add(item)

	q.dirty.insert(item)
	if q.processing.has(item) {
		return
	}

	q.queue[prio] = append(q.queue[prio], item)
	q.valid[prio] = append(q.valid[prio], true)

	//klog.Infof("%s GREPTAG Item added at prio signaling %d", q.name, prio)
	q.isempty = false

	q.cond.Signal()
}

// Len returns the current queue length, for informational purposes only. You
// shouldn't e.g. gate a call to Add() or Get() on Len() being a particular
// value, that can't be synchronized properly.
func (q *Type) Len() int {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	length := 0
	for i := 0; i < CRITICALITIES; i++ {
		length += len(q.queue[i])
	}
	return length
}

// Get blocks until it can return an item to be processed. If shutdown = true,
// the caller should end their goroutine. You must call Done with item when you
// have finished processing it.
// It assumes the element is valid, and returns the highest prio elem.
func (q *Type) Get() (item interface{}, shutdown bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	for q.isempty && !q.shuttingDown {
		q.cond.Wait()
	}
	if q.isempty {
		// We must be shutting down.
		return nil, true
	}

	var i int8
	for i = CRITICALITIES - 1; i >= 0; i-- {
		if len(q.queue[i]) != 0 {
			item = q.queue[i][0]
			// The underlying array still exists and reference this object,
			// so the object will not be garbage collected.
			q.queue[i][0] = nil
			q.queue[i] = q.queue[i][1:]
			q.valid[i] = q.valid[i][1:]
			//klog.Infof("%s Item found at prio %d", q.name, i)
			break
		}
	}

	//TODO improve this part
	length := 0
	for i >= 0 {
		length += len(q.queue[i])
		i--
		//klog.Infof("%s Status prio %d %d", q.name, i, len(q.queue[i]))
	}

	if length == 0 {
		q.isempty = true
		//klog.Infof("Empty queue ")
	}

	q.metrics.get(item)

	q.processing.insert(item)
	q.dirty.delete(item)

	return item, false
}

// GetDeterministic is non blocking, returns special codes for aborted exec
// If shutdown = true, the caller should end their goroutine.
// You must call Done with item when you
// have finished processing it.
// codes are: 0 ok, 1 shutdown, 2 empty, 3 invalid
// It returns the highest prio elem.
func (q *Type) GetDeterministic() (item interface{}, code int8) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	if q.isempty && !q.shuttingDown {
		return nil, 2
	}

	if q.isempty {
		// We must be shutting down.
		return nil, 1
	}

	var i int8
	for i = CRITICALITIES - 1; i >= 0; i-- {
		if len(q.queue[i]) != 0 {
			if q.valid[i][0] {
				item = q.queue[i][0]
				// The underlying array still exists and reference this object,
				// so the object will not be garbage collected.
				q.queue[i][0] = nil
				q.queue[i] = q.queue[i][1:]
				q.valid[i] = q.valid[i][1:]
				//klog.Infof("%s Item found at prio %d", q.name, i)
				break
			} else {
				return nil, 3
				//TODO: handle remotion of never-validated items after retrials
				// add another function?
			}
		}
	}

	//TODO improve this part
	length := 0
	for i >= 0 {
		length += len(q.queue[i])
		i--
		//klog.Infof("%s Status prio %d %d", q.name, i, len(q.queue[i]))
	}
	if length == 0 {
		q.isempty = true
		//klog.Infof("Empty queue ")
	}

	q.metrics.get(item)

	q.processing.insert(item)
	q.dirty.delete(item)

	return item, 0
}

// Done marks item as done processing, and if it has been marked as dirty again
// while it was being processed, it will be re-added to the queue for
// re-processing.
func (q *Type) Done(item interface{}, priority ...int) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	q.metrics.done(item)

	q.processing.delete(item)
	if q.dirty.has(item) {
		prio := 0
		if len(priority) > 0 {
			prio = priority[0]
			if prio > CRITICALITIES-1 {
				prio = CRITICALITIES - 1
			}
		}
		q.queue[prio] = append(q.queue[prio], item)
		q.valid[prio] = append(q.valid[prio], true)
		//klog.Infof("%s Item done at prio %d", q.name, prio)
		q.isempty = false
		q.cond.Signal()
	} else if q.processing.len() == 0 {
		q.cond.Signal()
	}
}

// ShutDown will cause q to ignore all new items added to it and
// immediately instruct the worker goroutines to exit.
func (q *Type) ShutDown() {
	q.setDrain(false)
	q.shutdown()
}

// ShutDownWithDrain will cause q to ignore all new items added to it. As soon
// as the worker goroutines have "drained", i.e: finished processing and called
// Done on all existing items in the queue; they will be instructed to exit and
// ShutDownWithDrain will return. Hence: a strict requirement for using this is;
// your workers must ensure that Done is called on all items in the queue once
// the shut down has been initiated, if that is not the case: this will block
// indefinitely. It is, however, safe to call ShutDown after having called
// ShutDownWithDrain, as to force the queue shut down to terminate immediately
// without waiting for the drainage.
func (q *Type) ShutDownWithDrain() {
	q.setDrain(true)
	q.shutdown()
	for q.isProcessing() && q.shouldDrain() {
		q.waitForProcessing()
	}
}

// isProcessing indicates if there are still items on the work queue being
// processed. It's used to drain the work queue on an eventual shutdown.
func (q *Type) isProcessing() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.processing.len() != 0
}

// waitForProcessing waits for the worker goroutines to finish processing items
// and call Done on them.
func (q *Type) waitForProcessing() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	// Ensure that we do not wait on a queue which is already empty, as that
	// could result in waiting for Done to be called on items in an empty queue
	// which has already been shut down, which will result in waiting
	// indefinitely.
	if q.processing.len() == 0 {
		return
	}
	q.cond.Wait()
}

func (q *Type) setDrain(shouldDrain bool) {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.drain = shouldDrain
}

func (q *Type) shouldDrain() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	return q.drain
}

func (q *Type) shutdown() {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()
	q.shuttingDown = true
	q.cond.Broadcast()
}

func (q *Type) ShuttingDown() bool {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	return q.shuttingDown
}

func (q *Type) updateUnfinishedWorkLoop() {
	t := q.clock.NewTicker(q.unfinishedWorkUpdatePeriod)
	defer t.Stop()
	for range t.C() {
		if !func() bool {
			q.cond.L.Lock()
			defer q.cond.L.Unlock()
			if !q.shuttingDown {
				q.metrics.updateUnfinishedWork()
				return true
			}
			return false

		}() {
			return
		}
	}
}
