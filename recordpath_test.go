package sqlair

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenizeRecordPath(t *testing.T) {
	tests := []struct {
		path   string
		result string
		err    error
	}{{
		path: "",
	}, {
		path:   "Person",
		result: "Person;",
	}, {
		path:   "Person.name",
		result: "Person.name;",
	}, {
		path:   "Person.name.head",
		result: "Person.name.head;",
	}, {
		path:   "Person[0].name.head",
		result: "(Person[0]).name.head;",
	}, {
		path:   "Person[0].name[1].head",
		result: "((Person[0]).name[1]).head;",
	}, {
		path:   "(Person[0]).name",
		result: "(Person[0]).name;",
	}, {
		path:   "name_with_underscores",
		result: "name_with_underscores;",
	}, {
		path:   "Person.*",
		result: "Person.*;",
	}, {
		path:   "Person.*.name[1]",
		result: "(Person.*.name[1]);",
	}, {
		path:   "Person.*.name[1] ",
		result: "(Person.*.name[1]);",
	}, {
		path:   "Person.*.name[1] AS",
		result: "(Person.*.name[1]);AS;",
	}}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			got, err := tokenizeRecordPath(test.path, 0)
			assert.Equal(t, test.err, err)
			assert.Equal(t, test.result, got.String())
		})
	}
}

func TestParseRecordPath(t *testing.T) {
	tests := []struct {
		path   string
		result []recordPath
		err    error
	}{{
		path: "",
	}, {
		path: "Person",
		result: []recordPath{
			makeRecordPathIdent("Person"),
		},
	}, {
		path: "Person.name",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathIdent("name"),
		},
	}, {
		path: "Person.name.head",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathIdent("name"),
			makeRecordPathIdent("head"),
		},
	}, {
		path: "Person[0].name.head",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathInteger(0),
			makeRecordPathIdent("name"),
			makeRecordPathIdent("head"),
		},
	}, {
		path: "Person[0].name[1].head",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathInteger(0),
			makeRecordPathIdent("name"),
			makeRecordPathInteger(1),
			makeRecordPathIdent("head"),
		},
	}, {
		path: "Person.*",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathIdent("*"),
		},
	}, {
		path: "Person.*.name[1]",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathIdent("*"),
			makeRecordPathIdent("name"),
			makeRecordPathInteger(1),
		},
	}, {
		path: "Person.*.name[1] AS",
		err:  ErrTooMany,
	}}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			got, err := parseRecordPath(test.path, 0)
			assert.Equal(t, test.err, err)
			assert.Equal(t, test.result, got)
		})
	}
}
