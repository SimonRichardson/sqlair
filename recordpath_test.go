package sqlair

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTokenizeRecordPath(t *testing.T) {
	tests := []struct {
		path     string
		result   string
		consumed int
		err      error
	}{{
		path:     "",
		consumed: 0,
	}, {
		path:     "Person",
		result:   "Person",
		consumed: 6,
	}, {
		path:     "Person.name",
		result:   "Person.name",
		consumed: 11,
	}, {
		path:     "Person.name.head",
		result:   "Person.name.head",
		consumed: 16,
	}, {
		path:     "Person[0].name.head",
		result:   "(Person[0]).name.head",
		consumed: 19,
	}, {
		path:     "Person[0].name[1].head",
		result:   "((Person[0]).name[1]).head",
		consumed: 22,
	}, {
		path:     "(Person[0]).name",
		result:   "(Person[0]).name",
		consumed: 16,
	}, {
		path:     "name_with_underscores",
		result:   "name_with_underscores",
		consumed: 21,
	}, {
		path:     "Person.*",
		result:   "Person.*",
		consumed: 8,
	}, {
		path:     "Person.*.name[1]",
		result:   "(Person.*.name[1])",
		consumed: 16,
	}, {
		path:     "Person.*.name[1] ",
		result:   "(Person.*.name[1])",
		consumed: 16,
	}, {
		path:     "Person.*.name[1] AS",
		result:   "(Person.*.name[1])",
		consumed: 16,
	}}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			got, consumed, err := tokenizeRecordPath(test.path, 0)
			assert.Equal(t, test.err, err)
			if got == nil && test.result == "" {
				return
			}
			assert.Equal(t, test.result, got.String())
			assert.Equal(t, test.consumed, consumed)
		})
	}
}

func TestParseRecordPath(t *testing.T) {
	tests := []struct {
		path     string
		result   []recordPath
		consumed int
		err      error
	}{{
		path:     "",
		consumed: 0,
	}, {
		path: "Person",
		result: []recordPath{
			makeRecordPathIdent("Person"),
		},
		consumed: 6,
	}, {
		path: "Person.name",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathIdent("name"),
		},
		consumed: 11,
	}, {
		path: "Person.name.head",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathIdent("name"),
			makeRecordPathIdent("head"),
		},
		consumed: 16,
	}, {
		path: "Person[0].name.head",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathInteger(0),
			makeRecordPathIdent("name"),
			makeRecordPathIdent("head"),
		},
		consumed: 19,
	}, {
		path: "Person[0].name[1].head",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathInteger(0),
			makeRecordPathIdent("name"),
			makeRecordPathInteger(1),
			makeRecordPathIdent("head"),
		},
		consumed: 22,
	}, {
		path: "Person.*",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathIdent("*"),
		},
		consumed: 8,
	}, {
		path: "Person.*.name[1]",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathIdent("*"),
			makeRecordPathIdent("name"),
			makeRecordPathInteger(1),
		},
		consumed: 16,
	}, {
		path: "Person.*.name[1] AS",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathIdent("*"),
			makeRecordPathIdent("name"),
			makeRecordPathInteger(1),
		},
		consumed: 16,
	}, {
		path: "Person.*.name[1]   	      ",
		result: []recordPath{
			makeRecordPathIdent("Person"),
			makeRecordPathIdent("*"),
			makeRecordPathIdent("name"),
			makeRecordPathInteger(1),
		},
		consumed: 16,
	}}
	for _, test := range tests {
		t.Run(test.path, func(t *testing.T) {
			got, consumed, err := parseRecordPath(test.path, 0)
			assert.Equal(t, test.err, err)
			assert.Equal(t, test.result, got)
			assert.Equal(t, test.consumed, consumed)
		})
	}
}
