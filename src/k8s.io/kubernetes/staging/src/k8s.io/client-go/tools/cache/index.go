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

	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/util/sets"
)

// Indexer is a storage interface that lets you list objects using multiple indexing functions
type Indexer interface { // 存储索引，提供多种方法检索
	Store  // 继承Store接口，表明Indexer也拥有Store一样的接口
	// Retrieve list of objects that match on the named indexing function
	Index(indexName string, obj interface{}) ([]interface{}, error)
	// IndexKeys returns the set of keys that match on the named indexing function.
	IndexKeys(indexName, indexKey string) ([]string, error)
	// ListIndexFuncValues returns the list of generated values of an Index func
	ListIndexFuncValues(indexName string) []string
	// ByIndex lists object that match on the named indexing function with the exact key
	ByIndex(indexName, indexKey string) ([]interface{}, error)
	// GetIndexer return the indexers
	GetIndexers() Indexers

	// AddIndexers adds more indexers to this store.  If you call this after you already have data
	// in the store, the results are undefined.
	AddIndexers(newIndexers Indexers) error
}

// IndexFunc knows how to provide an indexed value for an object.
type IndexFunc func(obj interface{}) ([]string, error)  // 计算索引的函数，每个对象可以计算出多个索引。（比如对象是一只猫，计算出的索引可能包含[猫科,小型动物,温顺]）

// IndexFuncToKeyFuncAdapter adapts an indexFunc to a keyFunc.  This is only useful if your index function returns
// unique values for every object.  This is conversion can create errors when more than one key is found.  You
// should prefer to make proper key and index functions.
func IndexFuncToKeyFuncAdapter(indexFunc IndexFunc) KeyFunc {
	return func(obj interface{}) (string, error) {
		indexKeys, err := indexFunc(obj)
		if err != nil {
			return "", err
		}
		if len(indexKeys) > 1 {
			return "", fmt.Errorf("too many keys: %v", indexKeys)
		}
		if len(indexKeys) == 0 {
			return "", fmt.Errorf("unexpected empty indexKeys")
		}
		return indexKeys[0], nil
	}
}

const (
	NamespaceIndex string = "namespace"
)

// MetaNamespaceIndexFunc is a default index function that indexes based on an object's namespace
func MetaNamespaceIndexFunc(obj interface{}) ([]string, error) { // 这就是一个典型的索引创建器，它跟据对象的namspace创建索引
	meta, err := meta.Accessor(obj)
	if err != nil {
		return []string{""}, fmt.Errorf("object has no meta: %v", err)
	}
	return []string{meta.GetNamespace()}, nil // 获取对象的namespace作为索引
}

// Index maps the indexed value to a set of keys in the store that match on that value
type Index map[string]sets.String  // 索引，索引可以有多个键，一般为一个。比如namespace，对应的value为该namespace下的对象

// Indexers maps a name to a IndexFunc
type Indexers map[string]IndexFunc // 索引器map，每个索引器都给指定对象分析出0~N个索引，一般一个索引器只分析出一个索引，比如"namespace"索引器，会把对象的namespace作为索引键。

// Indices maps a name to an Index
type Indices map[string]Index      // 保存索引器的索引
