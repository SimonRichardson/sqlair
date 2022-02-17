package reflect

import (
	"reflect"
	"runtime"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

type ReflectInfo interface {
	Kind() reflect.Kind
}

type ReflectValue struct {
	Value reflect.Value
}

func (r ReflectValue) Kind() reflect.Kind {
	return r.Value.Kind()
}

type ReflectTag struct {
	Name      string
	OmitEmpty bool
}

type ReflectField struct {
	Name  string
	Tag   ReflectTag
	Value reflect.Value
}

type ReflectStruct struct {
	Name   string
	Fields map[string]ReflectField
	Value  reflect.Value
}

func (r ReflectStruct) Kind() reflect.Kind {
	return r.Value.Kind()
}

// FieldNames returns the field names for a given type.
func (r ReflectStruct) FieldNames() []string {
	names := make([]string, 0, len(r.Fields))
	for name := range r.Fields {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Reflect parses a reflect.Value returning a ReflectInfo of fields and tags
// for the reflect value.
func Reflect(value reflect.Value) (ReflectInfo, error) {
	// Dereference the pointer if it is one.
	value = reflect.Indirect(value)
	if value.Kind() != reflect.Struct {
		return ReflectValue{
			Value: value,
		}, nil
	}

	refStruct := ReflectStruct{
		Name:   value.Type().Name(),
		Fields: make(map[string]ReflectField),
		Value:  value,
	}

	typ := value.Type()
	for i := 0; i < typ.NumField(); i++ {
		field := typ.Field(i)
		rawTag := field.Tag.Get("db")
		tag, err := parseTag(rawTag)
		if err != nil {
			return nil, err
		}

		name := tag.Name
		if name == "" {
			name = strings.ToLower(field.Name)
		}

		refStruct.Fields[name] = ReflectField{
			Name:  field.Name,
			Tag:   tag,
			Value: value.Field(i),
		}
	}

	return refStruct, nil
}

func parseTag(tag string) (ReflectTag, error) {
	if tag == "" {
		return ReflectTag{}, errors.Errorf("unexpected empty tag")
	}

	var refTag ReflectTag
	options := strings.Split(tag, ",")
	switch len(options) {
	case 2:
		if strings.ToLower(options[1]) != "omitempty" {
			return ReflectTag{}, errors.Errorf("unexpected tag value %q", options[1])
		}
		refTag.OmitEmpty = true
		fallthrough
	case 1:
		refTag.Name = options[0]
	}
	return refTag, nil
}

// methodName returns the caller of the function calling methodName
func methodName() string {
	pc, _, _, _ := runtime.Caller(2)
	f := runtime.FuncForPC(pc)
	if f == nil {
		return "unknown method"
	}
	return f.Name()
}
