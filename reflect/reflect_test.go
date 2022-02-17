package reflect

import (
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReflect(t *testing.T) {
	s := struct {
		ID   int64  `db:"id"`
		Name string `db:"name,omitempty"`
	}{}
	info, err := Reflect(reflect.ValueOf(&s))
	assert.Nil(t, err)

	structMap, ok := info.(ReflectStruct)
	assert.True(t, ok, true)

	assert.Len(t, structMap.Fields, 2)
	assert.Equal(t, structMap.FieldNames(), []string{"id", "name"})
}
