/*
Copyright 2014 The Kubernetes Authors.

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

	"k8s.io/apimachinery/pkg/util/sets"
)

// ThreadSafeStore is an interface that allows concurrent access to a storage backend.   // ThreadSafeStore 提供并发访问后端存储的并发接口
// TL;DR caveats: you must not modify anything returned by Get or List as it will break  // 警告: 不能修改任何Get() List()返回的数据,否则会影响索引功能
// the indexing feature in addition to not being thread safe.
//
// The guarantees of thread safety provided by List/Get are only valid if the caller     // 线程安全的前提是只能把Get() List()返回的数据当成只读的.
// treats returned items as read-only. For example, a pointer inserted in the store      // 例如,如果存储的是指针,同一个指针可以被多个client同时读出,如果修改的话，（会有读写冲突），就变成线程不安全了。
// through `Add` will be returned as is by `Get`. Multiple clients might invoke `Get`
// on the same key and modify the pointer in a non-thread-safe way. Also note that       // 另外，直接修改对象，也不会触发重新索引，所以一般不要修改Get() List()返回的数据。
// modifying objects stored by the indexers (if any) will *not* automatically lead
// to a re-index. So it's not a good idea to directly modify the objects returned by
// Get/List, in general.
type ThreadSafeStore interface {
	Add(key string, obj interface{})
	Update(key string, obj interface{})
	Delete(key string)
	Get(key string) (item interface{}, exists bool)
	List() []interface{}
	ListKeys() []string
	Replace(map[string]interface{}, string)
	Index(indexName string, obj interface{}) ([]interface{}, error)
	IndexKeys(indexName, indexKey string) ([]string, error)
	ListIndexFuncValues(name string) []string
	ByIndex(indexName, indexKey string) ([]interface{}, error)
	GetIndexers() Indexers

	// AddIndexers adds more indexers to this store.  If you call this after you already have data
	// in the store, the results are undefined.
	AddIndexers(newIndexers Indexers) error
	Resync() error
}

// threadSafeMap implements ThreadSafeStore
type threadSafeMap struct { // 线程安全存储，具体的对象存储在items，索引用于快速找出某一类对象的key列表，然后再访问items
	lock  sync.RWMutex
	items map[string]interface{} // 存储具体的对象，key-obj

	// indexers maps a name to an IndexFunc
	indexers Indexers // 索引函数集合，每个索引函数可以创建1到多个索引，一般每个索引函数创建一个索引，比如namespace索引函数，会分析对象的namespace，不同的对象对应不同的namespace，比如"default"、"kube-system"等
	// indices maps a name to an Index
	indices Indices // 索引集合，第一层对应索引函数，第二层为具体索引函数创建的索引集合
}

func (c *threadSafeMap) Add(key string, obj interface{}) { // key 比如：“kube-system/kube-dns”、“kube-system/pod-garbage-collector”、“kube-system/pv-protection-controller”、“default/kubernetes”等
	c.lock.Lock()
	defer c.lock.Unlock()
	oldObject := c.items[key]            // 先取出原对象(原对象可能不存在，此时oldObject会是nil，也不需要判断，统一交给updateIndices处理)
	c.items[key] = obj                   // 写入新的对象
	c.updateIndices(oldObject, obj, key) // 更新对象的索引
}

func (c *threadSafeMap) Update(key string, obj interface{}) { // Update与Add处理逻辑一致
	c.lock.Lock()
	defer c.lock.Unlock()
	oldObject := c.items[key]
	c.items[key] = obj
	c.updateIndices(oldObject, obj, key)
}

func (c *threadSafeMap) Delete(key string) { // 删除对象，内部会自动删除索引
	c.lock.Lock()
	defer c.lock.Unlock()
	if obj, exists := c.items[key]; exists {
		c.deleteFromIndices(obj, key) // 删除索引
		delete(c.items, key)          // 删除对象
	}
}

func (c *threadSafeMap) Get(key string) (item interface{}, exists bool) { // 跟据key值获取对象，此时无关索引，直接从map中查找
	c.lock.RLock()
	defer c.lock.RUnlock()
	item, exists = c.items[key]
	return item, exists
}

func (c *threadSafeMap) List() []interface{} { // 列出存储中全部的对象，以切片输出
	c.lock.RLock()
	defer c.lock.RUnlock()
	list := make([]interface{}, 0, len(c.items))
	for _, item := range c.items {
		list = append(list, item)
	}
	return list
}

// ListKeys returns a list of all the keys of the objects currently
// in the threadSafeMap.
func (c *threadSafeMap) ListKeys() []string { // 列出存储中全部对象的key值，以切片输出
	c.lock.RLock()
	defer c.lock.RUnlock()
	list := make([]string, 0, len(c.items))
	for key := range c.items {
		list = append(list, key)
	}
	return list
}

func (c *threadSafeMap) Replace(items map[string]interface{}, resourceVersion string) { // 完全替换新的元素集合，原索引直接抛弃，新元素重新建索引
	c.lock.Lock()
	defer c.lock.Unlock()
	c.items = items

	// rebuild any index
	c.indices = Indices{}
	for key, item := range c.items {
		c.updateIndices(nil, item, key)
	}
}

// Index returns a list of items that match on the index function
// Index is thread-safe so long as you treat all items as immutable
func (c *threadSafeMap) Index(indexName string, obj interface{}) ([]interface{}, error) { // 按照指定索引函数查找对象，此处传递obj，用于生成索引
	c.lock.RLock()
	defer c.lock.RUnlock()

	indexFunc := c.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	indexKeys, err := indexFunc(obj)
	if err != nil {
		return nil, err
	}
	index := c.indices[indexName]

	var returnKeySet sets.String
	if len(indexKeys) == 1 { // 由于大多数情况下只有一个索引键，这个分支用于一次把key集合全部取出来，为性能考虑
		// In majority of cases, there is exactly one value matching.
		// Optimize the most common path - deduping is not needed here.
		returnKeySet = index[indexKeys[0]]
	} else {
		// Need to de-dupe the return list.
		// Since multiple keys are allowed, this can happen.
		returnKeySet = sets.String{}
		for _, indexKey := range indexKeys {
			for key := range index[indexKey] {
				returnKeySet.Insert(key)
			}
		}
	}

	list := make([]interface{}, 0, returnKeySet.Len())
	for absoluteKey := range returnKeySet { // 跟据找出的key查找对象（不用遍历map，是索引性能提升的体现）
		list = append(list, c.items[absoluteKey])
	}
	return list, nil
}

// ByIndex returns a list of items that match an exact value on the index function
func (c *threadSafeMap) ByIndex(indexName, indexKey string) ([]interface{}, error) { // 找出指定索引名字、索引键查找对象
	c.lock.RLock()
	defer c.lock.RUnlock()

	indexFunc := c.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	index := c.indices[indexName]

	set := index[indexKey]
	list := make([]interface{}, 0, set.Len())
	for key := range set {
		list = append(list, c.items[key])
	}

	return list, nil
}

// IndexKeys returns a list of keys that match on the index function.
// IndexKeys is thread-safe so long as you treat all items as immutable.
func (c *threadSafeMap) IndexKeys(indexName, indexKey string) ([]string, error) { // 找出指定索引名字、索引键查找key列表
	c.lock.RLock()
	defer c.lock.RUnlock()

	indexFunc := c.indexers[indexName]
	if indexFunc == nil {
		return nil, fmt.Errorf("Index with name %s does not exist", indexName)
	}

	index := c.indices[indexName]

	set := index[indexKey]
	return set.List(), nil
}

func (c *threadSafeMap) ListIndexFuncValues(indexName string) []string { // 找出指定索引名字的索引键
	c.lock.RLock()
	defer c.lock.RUnlock()

	index := c.indices[indexName]
	names := make([]string, 0, len(index))
	for key := range index {
		names = append(names, key)
	}
	return names
}

func (c *threadSafeMap) GetIndexers() Indexers { // 返回索引函数集合
	return c.indexers
}

func (c *threadSafeMap) AddIndexers(newIndexers Indexers) error { // 增加新的索引函数集合，即便增加了新的索引函数集合，也不会把既有对象索引
	c.lock.Lock()
	defer c.lock.Unlock()

	if len(c.items) > 0 {
		return fmt.Errorf("cannot add indexers to running index")
	}

	oldKeys := sets.StringKeySet(c.indexers)
	newKeys := sets.StringKeySet(newIndexers)

	if oldKeys.HasAny(newKeys.List()...) { // 两索引函数集合如果有交集，则报冲突
		return fmt.Errorf("indexer conflict: %v", oldKeys.Intersection(newKeys))
	}

	for k, v := range newIndexers { // 如果没有冲突，则合并
		c.indexers[k] = v
	}
	return nil
}

// updateIndices modifies the objects location in the managed indexes, if this is an update, you must provide an oldObj
// updateIndices must be called from a function that already has a lock on the cache
func (c *threadSafeMap) updateIndices(oldObj interface{}, newObj interface{}, key string) { // 更新索引
	// if we got an old object, we need to remove it before we add it again
	if oldObj != nil { // 如果原对象不为空，即非首次添加而是更新，需要把原对象的索引删除
		c.deleteFromIndices(oldObj, key)
	}
	for name, indexFunc := range c.indexers { // 便利索引函数，按索引函数划分的索引创建索引
		indexValues, err := indexFunc(newObj) // 生成索引. 比如namespace索引函数会生成对象的索引，比如key为"kube-system/kube-dns"，此处生成的索引为[kube-system]，虽然为切片，但往往只有一个元素
		if err != nil {
			panic(fmt.Errorf("unable to calculate an index entry for key %q on index %q: %v", key, name, err))
		}
		index := c.indices[name] // 找到该索引函数对应的索引集合
		if index == nil {        // 如果没有，需要创建
			index = Index{}
			c.indices[name] = index
		}

		for _, indexValue := range indexValues { //
			set := index[indexValue] // 找到指定索引位置，set即为指定索引的key集合，比如同属于kube-system下的多个key值
			if set == nil {
				set = sets.String{}
				index[indexValue] = set
			}
			set.Insert(key) // 插入新的key值
		}
	}
}

// deleteFromIndices removes the object from each of the managed indexes
// it is intended to be called from a function that already has a lock on the cache
func (c *threadSafeMap) deleteFromIndices(obj interface{}, key string) { // 删除对象的索引
	for name, indexFunc := range c.indexers { // 遍历多个索引器，将对象的key从每个索引器的多个索引中删除
		indexValues, err := indexFunc(obj) // 让索引器计算出对象的索引（可能有多个）
		if err != nil {
			panic(fmt.Errorf("unable to calculate an index entry for key %q on index %q: %v", key, name, err))
		}

		index := c.indices[name] // 如果该对象没有索引，忽略后续
		if index == nil {
			continue
		}
		for _, indexValue := range indexValues { // 遍历索引值,从索引集合中逐个删除元素key值
			set := index[indexValue]
			if set != nil {
				set.Delete(key)
			}
		}
	}
}

func (c *threadSafeMap) Resync() error {
	// Nothing to do
	return nil
}

func NewThreadSafeStore(indexers Indexers, indices Indices) ThreadSafeStore {
	return &threadSafeMap{
		items:    map[string]interface{}{},
		indexers: indexers,
		indices:  indices,
	}
}
