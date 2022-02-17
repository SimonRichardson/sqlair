package reflect

import (
	"reflect"
	"sync"
)

// ReflectCache caches the types for faster look up times.
type ReflectCache struct {
	mutex sync.RWMutex
	cache map[reflect.Type]ReflectInfo
}

// NewReflectCache creates a new ReflectCache that caches the types for faster
// look up times.
func NewReflectCache() *ReflectCache {
	return &ReflectCache{
		cache: make(map[reflect.Type]ReflectInfo),
	}
}

// Reflect will return a Reflectstruct of a given type.
func (r *ReflectCache) Reflect(value interface{}) (ReflectInfo, error) {
	raw := reflect.ValueOf(value)
	mustBe(raw, reflect.Ptr)

	v := reflect.Indirect(raw)
	r.mutex.Lock()
	defer r.mutex.Unlock()

	if rs, ok := r.cache[v.Type()]; ok {
		return rs, nil
	}

	ri, err := Reflect(v)
	if err != nil {
		return ReflectStruct{}, err
	}
	r.cache[v.Type()] = ri
	return ri, nil
}

type kinder interface {
	Kind() reflect.Kind
}

// mustBe checks a value against a kind, panicing with a reflect.ValueError
// if the kind isn't that which is required.
func mustBe(v kinder, expected reflect.Kind) {
	if k := v.Kind(); k != expected {
		panic(&reflect.ValueError{Method: methodName(), Kind: k})
	}
}
