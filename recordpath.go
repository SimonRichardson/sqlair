package sqlair

import (
	"fmt"

	"github.com/SimonRichardson/sqlair/parser"
)

func tokenizeRecordPath(stmt string, offset int) (*parser.QueryExpression, int, error) {
	lexer := parser.NewLexer(stmt[offset:])
	parser := parser.NewParser(lexer)
	return parser.Run()
}

type recordPathType int

const (
	recordPathIdent recordPathType = iota
	recordPathInteger
	recordPathString
)

type recordPath struct {
	tokenType recordPathType
	value     interface{}
}

func makeRecordPathIdent(value string) recordPath {
	return recordPath{
		tokenType: recordPathIdent,
		value:     value,
	}
}

func makeRecordPathInteger(value int64) recordPath {
	return recordPath{
		tokenType: recordPathInteger,
		value:     value,
	}
}

func makeRecordPathString(value string) recordPath {
	return recordPath{
		tokenType: recordPathString,
		value:     value,
	}
}

func parseRecordPath(stmt string, offset int) ([]recordPath, int, error) {
	ast, consumed, err := tokenizeRecordPath(stmt, offset)
	if err != nil {
		return nil, consumed, err
	}

	paths, err := compileRecordPathAST(ast)
	if err != nil {
		return nil, -1, err
	}
	return paths, consumed, nil
}

var (
	ErrTooMany = fmt.Errorf("got more than one expression")
)

func compileRecordPathAST(ast parser.Expression) ([]recordPath, error) {
	switch node := ast.(type) {
	case *parser.QueryExpression:
		num := len(node.Expressions)
		if num == 0 {
			return nil, nil
		}

		result, err := compileRecordPathAST(node.Expressions[0])
		if err != nil {
			return nil, err
		}
		if num > 1 {
			return result, ErrTooMany
		}
		return result, nil

	case *parser.ExpressionStatement:
		return compileRecordPathAST(node.Expression)

	case *parser.IndexExpression:
		left, err := compileRecordPathAST(node.Left)
		if err != nil {
			return nil, err
		}

		index, err := compileRecordPathAST(node.Index)
		if err != nil {
			return nil, err
		}

		return append(left, index...), nil

	case *parser.AccessorExpression:
		left, err := compileRecordPathAST(node.Left)
		if err != nil {
			return nil, err
		}

		index, err := compileRecordPathAST(node.Right)
		if err != nil {
			return nil, err
		}

		return append(left, index...), nil

	case *parser.Identifier:
		return []recordPath{makeRecordPathIdent(node.Token.Literal)}, nil

	case *parser.Integer:
		return []recordPath{makeRecordPathInteger(node.Value)}, nil

	case *parser.String:
		return []recordPath{makeRecordPathString(node.Token.Literal)}, nil

	case *parser.Separator, *parser.Empty:
		return nil, nil
	}

	return nil, fmt.Errorf("syntax error: unexpected expression %T", ast)
}
