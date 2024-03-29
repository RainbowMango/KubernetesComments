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

package cache

import (
	"fmt"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/clock"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/buffer"

	"k8s.io/klog"
)

// SharedInformer provides eventually consistent linkage of its // SharedInformer提供客户端和一系列对象的一致性保证。
// clients to the authoritative state of a given collection of
// objects.  An object is identified by its API group, kind/resource, // 对象使用API group、kind/resource、namespace和name进行识别（使用其中的组合）
// namespace, and name.  One SharedInfomer provides linkage to objects // 每个SharedInformer 对应某个特定类型的资源
// of a particular API group and kind/resource.  The linked object  // SharedInformer管理的对象可能进一步跟据namespace、label selector、field selector做细分。
// collection of a SharedInformer may be further restricted to one
// namespace and/or by label selector and/or field selector.
//
// The authoritative state of an object is what apiservers provide  // 对象的权威状态是指apiserver中的状态，可能会有一系列的状态变迁。
// access to, and an object goes through a strict sequence of states.
// A state is either "absent" or present with a ResourceVersion and // 状态要么是"缺席"（未知），要么是其他合适的状态
// other appropriate content.
//
// A SharedInformer maintains a local cache, exposed by Store(), of  // SharedInformer 维护了一个本地缓存（通过GetStore()对外暴露），来保存相关对象的状态。
// the state of each relevant object.  This cache is eventually      // 缓存中的对象状态与apiserver中的一致
// consistent with the authoritative state.  This means that, unless // 除非永久出现连接问题，最终SharedInformer中对象的状态会与api server一致
// prevented by persistent communication problems, if ever a
// particular object ID X is authoritatively associated with a state S
// then for every SharedInformer I whose collection includes (X, S)
// eventually either (1) I's cache associates X with S or a later
// state of X, (2) I is stopped, or (3) the authoritative state
// service for X terminates.  To be formally complete, we say that the
// absent state meets any restriction by label selector or field
// selector.
//
// As a simple example, if a collection of objects is henceforeth  // 举个简单的例子，如果某种类型的对象都不变的情况下，SharedInformer缓存中将存储这些对象的副本。
// unchanging and a SharedInformer is created that links to that
// collection then that SharedInformer's cache eventually holds an
// exact copy of that collection (unless it is stopped too soon, the
// authoritative state service ends, or communication problems between
// the two persistently thwart achievement).
//
// As another simple example, if the local cache ever holds a     // 另一个例子，如果SharedInformer缓存中某些对象的状态为"non-absent"，且如果对象最终被删除的话，那么本地缓存中也会把它删除。
// non-absent state for some object ID and the object is eventually
// removed from the authoritative state then eventually the object is
// removed from the local cache (unless the SharedInformer is stopped
// too soon, the authoritative state service emnds, or communication
// problems persistently thwart the desired result).
//
// The keys in Store() are of the form namespace/name for namespaced // 缓存中的对象，namespace/name或name做为key值
// objects, and are simply the name for non-namespaced objects.
//
// A client is identified here by a ResourceEventHandler.  For every // 所谓的客户端也就是ResourceEventHandler。
// update to the SharedInformer's local cache and for every client,  // SharedInformer缓存的每次更新，都会发通知给客户端，除非SharedInformer停掉。
// eventually either the SharedInformer is stopped or the client is
// notified of the update.  These notifications happen after the     // 通知会在写入缓存后且创建完相应的索引后再下发（因为下发通知后马上会来查，此时索引还未建立，查询会失败或等待）
// corresponding cache update and, in the case of a
// SharedIndexInformer, after the corresponding index updates.  It is  // 发送通知前，可能还会有额外的缓存、索引更新
// possible that additional cache and index updates happen before such
// a prescribed notification.  For a given SharedInformer and client,  // SharedInformer 是按照顺序发送通知的。
// all notifications are delivered sequentially.  For a given
// SharedInformer, client, and object ID, the notifications are
// delivered in order.
//
// A delete notification exposes the last locally known non-absent // 删除通知意味着最后一个“non-absent”状态
// state, except that its ResourceVersion is replaced with a
// ResourceVersion in which the object is actually absent.
type SharedInformer interface {
	// AddEventHandler adds an event handler to the shared informer using the shared informer's resync // 向shared informer中添加一个事件处理者
	// period.  Events to a single handler are delivered sequentially, but there is no coordination    // 对于单个事件处理者而言，事件是逐个发送的，与其他事件处理者无关
	// between different handlers.
	AddEventHandler(handler ResourceEventHandler)
	// AddEventHandlerWithResyncPeriod adds an event handler to the // 向shared informer中添加一个事件处理者,同时指定resync周期
	// shared informer using the specified resync period.  The resync  // resync将把本地缓存中的对象全部按照Add事件发送出去
	// operation consists of delivering to the handler a create
	// notification for every object in the informer's local cache; it // resync不会改变apiserver中的数据
	// does not add any interactions with the authoritative storage.
	AddEventHandlerWithResyncPeriod(handler ResourceEventHandler, resyncPeriod time.Duration)
	// GetStore returns the informer's local cache as a Store.        // 对外暴露本地缓存
	GetStore() Store
	// GetController gives back a synthetic interface that "votes" to start the informer
	GetController() Controller
	// Run starts and runs the shared informer, returning after it stops.  // 启动informer,直到停止时才退出,可以使用stopCh退出
	// The informer will be stopped when stopCh is closed.
	Run(stopCh <-chan struct{})
	// HasSynced returns true if the shared informer's store has been // 标记是否已从apiserver完整的同步过一次数据(跟resync无关)
	// informed by at least one full LIST of the authoritative state
	// of the informer's object collection.  This is unrelated to "resync".
	HasSynced() bool
	// LastSyncResourceVersion is the resource version observed when last synced with the underlying // 本地缓存的资源版本,(可能本地缓存每次有数据变更时版本+1)
	// store. The value returned is not synchronized with access to the underlying store and is not
	// thread-safe.
	LastSyncResourceVersion() string
}

type SharedIndexInformer interface { // 基于SharedInformer生成
	SharedInformer
	// AddIndexers add indexers to the informer before it starts.
	AddIndexers(indexers Indexers) error
	GetIndexer() Indexer
}

// NewSharedInformer creates a new instance for the listwatcher.
func NewSharedInformer(lw ListerWatcher, objType runtime.Object, resyncPeriod time.Duration) SharedInformer { // 注意:创建SharedInformer实际上创建的是SharedIndexInformer
	return NewSharedIndexInformer(lw, objType, resyncPeriod, Indexers{})
}

// NewSharedIndexInformer creates a new instance for the listwatcher.
func NewSharedIndexInformer(lw ListerWatcher, objType runtime.Object, defaultEventHandlerResyncPeriod time.Duration, indexers Indexers) SharedIndexInformer {
	realClock := &clock.RealClock{}
	sharedIndexInformer := &sharedIndexInformer{
		processor:                       &sharedProcessor{clock: realClock},
		indexer:                         NewIndexer(DeletionHandlingMetaNamespaceKeyFunc, indexers),
		listerWatcher:                   lw,
		objectType:                      objType,
		resyncCheckPeriod:               defaultEventHandlerResyncPeriod,
		defaultEventHandlerResyncPeriod: defaultEventHandlerResyncPeriod,
		cacheMutationDetector:           NewCacheMutationDetector(fmt.Sprintf("%T", objType)),
		clock:                           realClock,
	}
	return sharedIndexInformer
}

// InformerSynced is a function that can be used to determine if an informer has synced.  This is useful for determining if caches have synced.
type InformerSynced func() bool

const (
	// syncedPollPeriod controls how often you look at the status of your sync funcs
	syncedPollPeriod = 100 * time.Millisecond

	// initialBufferSize is the initial number of event notifications that can be buffered.
	initialBufferSize = 1024
)

// WaitForCacheSync waits for caches to populate.  It returns true if it was successful, false
// if the controller should shutdown
func WaitForCacheSync(stopCh <-chan struct{}, cacheSyncs ...InformerSynced) bool {
	err := wait.PollUntil(syncedPollPeriod,
		func() (bool, error) {
			for _, syncFunc := range cacheSyncs {
				if !syncFunc() {
					return false, nil
				}
			}
			return true, nil
		},
		stopCh)
	if err != nil {
		klog.V(2).Infof("stop requested")
		return false
	}

	klog.V(4).Infof("caches populated")
	return true
}

type sharedIndexInformer struct {
	indexer    Indexer     // informer的索引器，informer从apiserver获取到的对象都会存在此处，还会为对象创建索引以加速查询
	controller Controller

	processor             *sharedProcessor  // 管理所有用户的回调函数，分发消息给用户
	cacheMutationDetector CacheMutationDetector

	// This block is tracked to handle late initialization of the controller
	listerWatcher ListerWatcher
	objectType    runtime.Object

	// resyncCheckPeriod is how often we want the reflector's resync timer to fire so it can call
	// shouldResync to check if any of our listeners need a resync.
	resyncCheckPeriod time.Duration // resync检查周期，即周期性的检查有没有需要resync的listener
	// defaultEventHandlerResyncPeriod is the default resync period for any handlers added via
	// AddEventHandler (i.e. they don't specify one and just want to use the shared informer's default
	// value).
	defaultEventHandlerResyncPeriod time.Duration
	// clock allows for testability
	clock clock.Clock

	started, stopped bool
	startedLock      sync.Mutex

	// blockDeltas gives a way to stop all event distribution so that a late event handler
	// can safely join the shared informer.
	blockDeltas sync.Mutex
}

// dummyController hides the fact that a SharedInformer is different from a dedicated one
// where a caller can `Run`.  The run method is disconnected in this case, because higher
// level logic will decide when to start the SharedInformer and related controller.
// Because returning information back is always asynchronous, the legacy callers shouldn't
// notice any change in behavior.
type dummyController struct {
	informer *sharedIndexInformer
}

func (v *dummyController) Run(stopCh <-chan struct{}) {
}

func (v *dummyController) HasSynced() bool {
	return v.informer.HasSynced()
}

func (c *dummyController) LastSyncResourceVersion() string {
	return ""
}

type updateNotification struct {
	oldObj interface{}
	newObj interface{}
}

type addNotification struct {
	newObj interface{}
}

type deleteNotification struct {
	oldObj interface{}
}

func (s *sharedIndexInformer) Run(stopCh <-chan struct{}) {
	defer utilruntime.HandleCrash()

	fifo := NewDeltaFIFO(MetaNamespaceKeyFunc, s.indexer)

	cfg := &Config{
		Queue:            fifo,
		ListerWatcher:    s.listerWatcher,
		ObjectType:       s.objectType,
		FullResyncPeriod: s.resyncCheckPeriod,
		RetryOnError:     false,
		ShouldResync:     s.processor.shouldResync,

		Process: s.HandleDeltas,
	}

	func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()

		s.controller = New(cfg)
		s.controller.(*controller).clock = s.clock
		s.started = true
	}()

	// Separate stop channel because Processor should be stopped strictly after controller
	processorStopCh := make(chan struct{})
	var wg wait.Group
	defer wg.Wait()              // Wait for Processor to stop
	defer close(processorStopCh) // Tell Processor to stop
	wg.StartWithChannel(processorStopCh, s.cacheMutationDetector.Run)
	wg.StartWithChannel(processorStopCh, s.processor.run)

	defer func() {
		s.startedLock.Lock()
		defer s.startedLock.Unlock()
		s.stopped = true // Don't want any new listeners
	}()
	s.controller.Run(stopCh)
}

func (s *sharedIndexInformer) HasSynced() bool {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.controller == nil {
		return false
	}
	return s.controller.HasSynced()
}

func (s *sharedIndexInformer) LastSyncResourceVersion() string {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.controller == nil {
		return ""
	}
	return s.controller.LastSyncResourceVersion()
}

func (s *sharedIndexInformer) GetStore() Store {
	return s.indexer
}

func (s *sharedIndexInformer) GetIndexer() Indexer {
	return s.indexer
}

func (s *sharedIndexInformer) AddIndexers(indexers Indexers) error {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.started {
		return fmt.Errorf("informer has already started")
	}

	return s.indexer.AddIndexers(indexers)
}

func (s *sharedIndexInformer) GetController() Controller {
	return &dummyController{informer: s}
}

func (s *sharedIndexInformer) AddEventHandler(handler ResourceEventHandler) { // 添加事件处理者，使用默认的resync周期
	s.AddEventHandlerWithResyncPeriod(handler, s.defaultEventHandlerResyncPeriod)
}

func determineResyncPeriod(desired, check time.Duration) time.Duration { // check意为上层检查(是否有resync的必要)的周期,实际resync周期一定要>= 检查周期。比如listener希望1s resync一次，但是检查周期为5s，即5s才可以开始一次resync，listener期望的1秒不生效
	if desired == 0 {
		return desired
	}
	if check == 0 {
		klog.Warningf("The specified resyncPeriod %v is invalid because this shared informer doesn't support resyncing", desired)
		return 0
	}
	if desired < check { // 期望的周期如果比检查周期小，则需要调整，记录warning日志
		klog.Warningf("The specified resyncPeriod %v is being increased to the minimum resyncCheckPeriod %v", desired, check)
		return check
	}
	return desired
}

const minimumResyncPeriod = 1 * time.Second

func (s *sharedIndexInformer) AddEventHandlerWithResyncPeriod(handler ResourceEventHandler, resyncPeriod time.Duration) {
	s.startedLock.Lock()
	defer s.startedLock.Unlock()

	if s.stopped {
		klog.V(2).Infof("Handler %v was not added to shared informer because it has stopped already", handler)
		return
	}

	if resyncPeriod > 0 {
		if resyncPeriod < minimumResyncPeriod { // resync周期不能小于1s,否则性能会有问题
			klog.Warningf("resyncPeriod %d is too small. Changing it to the minimum allowed value of %d", resyncPeriod, minimumResyncPeriod)
			resyncPeriod = minimumResyncPeriod
		}

		if resyncPeriod < s.resyncCheckPeriod {  // 如果新的用户希望更频繁的resync，那么检查周期也需要相应的变小（当informer还未启动的情况下）。
			if s.started { // informer已经启动的情况，新加入的用户不能使用比检查周期更小的resync周期，这里会修改用户的期望，所以要记录warning日志
				klog.Warningf("resyncPeriod %d is smaller than resyncCheckPeriod %d and the informer has already started. Changing it to %d", resyncPeriod, s.resyncCheckPeriod, s.resyncCheckPeriod)
				resyncPeriod = s.resyncCheckPeriod
			} else { // 如果informer还未启动,检查周期要变小，同时更新所有listeners的resync周期,同步更新所有listener的周期
				// if the event handler's resyncPeriod is smaller than the current resyncCheckPeriod, update
				// resyncCheckPeriod to match resyncPeriod and adjust the resync periods of all the listeners
				// accordingly
				s.resyncCheckPeriod = resyncPeriod
				s.processor.resyncCheckPeriodChanged(resyncPeriod)
			}
		}
	}

	listener := newProcessListener(handler, resyncPeriod, determineResyncPeriod(resyncPeriod, s.resyncCheckPeriod), s.clock.Now(), initialBufferSize)

	if !s.started { // 如果informer还未启动,则把listener添加进去就结束了,直接返回
		s.processor.addListener(listener)
		return
	}

	// in order to safely join, we have to
	// 1. stop sending add/update/delete notifications     // 1. 停止当前的通知发送动作
	// 2. do a list against the store                      // 2. 获取本地缓存中所有的对象列表
	// 3. send synthetic "Add" events to the new handler   // 3. 将列表以"Add"事件发送给新的handler
	// 4. unblock                                          // 4. 解锁,继续发送
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	s.processor.addListener(listener)
	for _, item := range s.indexer.List() {
		listener.add(addNotification{newObj: item})
	}
}

func (s *sharedIndexInformer) HandleDeltas(obj interface{}) error {
	s.blockDeltas.Lock()
	defer s.blockDeltas.Unlock()

	// from oldest to newest
	for _, d := range obj.(Deltas) {
		switch d.Type {
		case Sync, Added, Updated:
			isSync := d.Type == Sync
			s.cacheMutationDetector.AddObject(d.Object)
			if old, exists, err := s.indexer.Get(d.Object); err == nil && exists {
				if err := s.indexer.Update(d.Object); err != nil {
					return err
				}
				s.processor.distribute(updateNotification{oldObj: old, newObj: d.Object}, isSync)
			} else {
				if err := s.indexer.Add(d.Object); err != nil {
					return err
				}
				s.processor.distribute(addNotification{newObj: d.Object}, isSync)
			}
		case Deleted:
			if err := s.indexer.Delete(d.Object); err != nil {
				return err
			}
			s.processor.distribute(deleteNotification{oldObj: d.Object}, false)
		}
	}
	return nil
}

type sharedProcessor struct {
	listenersStarted bool  // listener启动标志，所有listener启停状态一致
	listenersLock    sync.RWMutex
	listeners        []*processorListener  // 所有的listener列表
	syncingListeners []*processorListener  // 需要resync的listener列表
	clock            clock.Clock
	wg               wait.Group
}

func (p *sharedProcessor) addListener(listener *processorListener) { //  添加新的listener
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	p.addListenerLocked(listener)
	if p.listenersStarted { // 如果其他的listener已经启动，则需要把新加进来的启动
		p.wg.Start(listener.run)
		p.wg.Start(listener.pop)
	}
}

func (p *sharedProcessor) addListenerLocked(listener *processorListener) { // 添加新的listener
	p.listeners = append(p.listeners, listener)
	p.syncingListeners = append(p.syncingListeners, listener) // 每个新加入的listener都加入resync列表中，是为了确保该listener能获取全量信息
}

func (p *sharedProcessor) distribute(obj interface{}, sync bool) { // 分发通知，跟据sync(resync)标志决定分发对象
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()

	if sync { // resync通知，分发给需要resync的listener
		for _, listener := range p.syncingListeners {
			listener.add(obj)
		}
	} else { // 非resync通知，发给所有listeners
		for _, listener := range p.listeners {
			listener.add(obj)
		}
	}
}

func (p *sharedProcessor) run(stopCh <-chan struct{}) {
	func() {
		p.listenersLock.RLock()
		defer p.listenersLock.RUnlock()
		for _, listener := range p.listeners {
			p.wg.Start(listener.run)
			p.wg.Start(listener.pop)
		}
		p.listenersStarted = true
	}()
	<-stopCh  // 启动后就阻塞等待上游关闭指令，唤醒时代表可以关闭所有的listener了
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()
	for _, listener := range p.listeners { // 向所有的listener发送关闭指令
		close(listener.addCh) // Tell .pop() to stop. .pop() will tell .run() to stop
	}
	p.wg.Wait() // Wait for all .pop() and .run() to stop // 等待所有的listener关闭
}

// shouldResync queries every listener to determine if any of them need a resync, based on each
// listener's resyncPeriod.
func (p *sharedProcessor) shouldResync() bool { // 判断是否需要启动resync。遍历所有的listener，把到了resync时间的listener加入p.syncingListeners列表。只要p.syncingListeners不为空则代表需要启动resync.
	p.listenersLock.Lock()
	defer p.listenersLock.Unlock()

	p.syncingListeners = []*processorListener{}

	resyncNeeded := false
	now := p.clock.Now()
	for _, listener := range p.listeners {
		// need to loop through all the listeners to see if they need to resync so we can prepare any
		// listeners that are going to be resyncing.
		if listener.shouldResync(now) {
			resyncNeeded = true
			p.syncingListeners = append(p.syncingListeners, listener)
			listener.determineNextResync(now) // listener加入resync列表后，立即计算下一次resync时间
		}
	}
	return resyncNeeded
}

func (p *sharedProcessor) resyncCheckPeriodChanged(resyncCheckPeriod time.Duration) { // 遍历所有的listener，设置同步周期
	p.listenersLock.RLock()
	defer p.listenersLock.RUnlock()

	for _, listener := range p.listeners {
		resyncPeriod := determineResyncPeriod(listener.requestedResyncPeriod, resyncCheckPeriod)
		listener.setResyncPeriod(resyncPeriod)
	}
}

type processorListener struct {
	nextCh chan interface{}  // 从buffer中取数据到这个管道,管道出口为调用事件处理函数
	addCh  chan interface{}  // 新消息到来时写到这个管道(管道出口为buffer)    notification --> addCh --> pendingNotification --> nextCh --> handler

	handler ResourceEventHandler  // 监听者的事件处理函数

	// pendingNotifications is an unbounded ring buffer that holds all notifications not yet distributed. // pendingNotifications是一个环形buffer，保存还没发出的通知
	// There is one per listener, but a failing/stalled listener will have infinite pendingNotifications // 每个listener一个buffer，如果listener处理消息慢或停掉了，那么消息会一直缓存在buffer中，最终会溢出
	// added until we OOM.
	// TODO: This is no worse than before, since reflectors were backed by unbounded DeltaFIFOs, but
	// we should try to do something better.
	pendingNotifications buffer.RingGrowing

	// requestedResyncPeriod is how frequently the listener wants a full resync from the shared informer
	requestedResyncPeriod time.Duration // 期望的resync周期，因为resync周期经常会因为其他listener的改变而改变，这里保留期望值，便于后续每次调整时重新审视
	// resyncPeriod is how frequently the listener wants a full resync from the shared informer. This
	// value may differ from requestedResyncPeriod if the shared informer adjusts it to align with the
	// informer's overall resync check period.
	resyncPeriod time.Duration // 实际的resync周期，该值需要与informer的resync周期保持一致
	// nextResync is the earliest time the listener should get a full resync
	nextResync time.Time // 下次resync的时间
	// resyncLock guards access to resyncPeriod and nextResync
	resyncLock sync.Mutex
}

func newProcessListener(handler ResourceEventHandler, requestedResyncPeriod, resyncPeriod time.Duration, now time.Time, bufferSize int) *processorListener {
	ret := &processorListener{
		nextCh:                make(chan interface{}),
		addCh:                 make(chan interface{}),
		handler:               handler,
		pendingNotifications:  *buffer.NewRingGrowing(bufferSize),
		requestedResyncPeriod: requestedResyncPeriod,
		resyncPeriod:          resyncPeriod,
	}

	ret.determineNextResync(now)

	return ret
}

func (p *processorListener) add(notification interface{}) { // 每添加一个通知，就把它放到管道中
	p.addCh <- notification
}

func (p *processorListener) pop() {
	defer utilruntime.HandleCrash()
	defer close(p.nextCh) // Tell .run() to stop

	var nextCh chan<- interface{}  // 注意，这里仅仅是声明，还没有创建管道
	var notification interface{}  // 注意，这里仅仅是声明，此时notification为nil
	for {
		select {
		case nextCh <- notification: // 分发一个通知，然后从buffer中再取一个
			// Notification dispatched
			var ok bool
			notification, ok = p.pendingNotifications.ReadOne()
			if !ok { // Nothing to pop
				nextCh = nil // Disable this select case
			}
		case notificationToAdd, ok := <-p.addCh: // 如果管道中来一个新数据，第一次将让nextCh指向 p.nextCh，之后直接写到buffer里
			if !ok {
				return
			}
			if notification == nil { // No notification to pop (and pendingNotifications is empty)
				// Optimize the case - skip adding to pendingNotifications
				notification = notificationToAdd
				nextCh = p.nextCh
			} else { // There is already a notification waiting to be dispatched
				p.pendingNotifications.WriteOne(notificationToAdd)
			}
		}
	}
}

func (p *processorListener) run() { // 启动无限循环，从p.nextCh获取数据，然后发送出去
	// this call blocks until the channel is closed.  When a panic happens during the notification
	// we will catch it, **the offending item will be skipped!**, and after a short delay (one second)
	// the next notification will be attempted.  This is usually better than the alternative of never
	// delivering again.
	stopCh := make(chan struct{})
	wait.Until(func() {
		// this gives us a few quick retries before a long pause and then a few more quick retries
		err := wait.ExponentialBackoff(retry.DefaultRetry, func() (bool, error) {
			for next := range p.nextCh {
				switch notification := next.(type) { // 跟据不同通知类型，调用相应的回调函数
				case updateNotification:
					p.handler.OnUpdate(notification.oldObj, notification.newObj)
				case addNotification:
					p.handler.OnAdd(notification.newObj)
				case deleteNotification:
					p.handler.OnDelete(notification.oldObj)
				default:
					utilruntime.HandleError(fmt.Errorf("unrecognized notification: %T", next))
				}
			}
			// the only way to get here is if the p.nextCh is empty and closed
			return true, nil
		})

		// the only way to get here is if the p.nextCh is empty and closed
		if err == nil {  // 当走到这里时说明p.nextCh已关闭，即需要停止了，把stopCh关闭，防止Until再次执行
			close(stopCh)
		}
	}, 1*time.Minute, stopCh)
}

// shouldResync deterimines if the listener needs a resync. If the listener's resyncPeriod is 0,
// this always returns false.
func (p *processorListener) shouldResync(now time.Time) bool { // 跟据时间判断是否需要resync
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	if p.resyncPeriod == 0 { // resync周期为0，等同于不同步，返回false。
		return false
	}

	return now.After(p.nextResync) || now.Equal(p.nextResync) // 如果下次同步时间已过,则返回true
}

func (p *processorListener) determineNextResync(now time.Time) { // 计算下次resync的时间，当前时间+同步周期resyncPeriod
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	p.nextResync = now.Add(p.resyncPeriod)
}

func (p *processorListener) setResyncPeriod(resyncPeriod time.Duration) { // 设置resync周期，内部方法
	p.resyncLock.Lock()
	defer p.resyncLock.Unlock()

	p.resyncPeriod = resyncPeriod
}
