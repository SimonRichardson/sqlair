package parser

import (
	"bytes"
	"fmt"
	"strings"
	"unicode/utf8"
)

// Expression defines a type of AST node for outlining an expression.
type Expression interface {
	Pos() Position
	End() Position

	String() string
}

// QueryExpression represents a query full of expressions
type QueryExpression struct {
	Expressions []Expression
}

// Pos returns the first position of the query expression.
func (e *QueryExpression) Pos() Position {
	if len(e.Expressions) > 0 {
		return e.Expressions[0].Pos()
	}
	return Position{}
}

// End returns the last position of the query expression.
func (e *QueryExpression) End() Position {
	if num := len(e.Expressions); num > 0 {
		return e.Expressions[num-1].End()
	}
	return Position{}
}

func (e *QueryExpression) String() string {
	var out bytes.Buffer

	for _, s := range e.Expressions {
		out.WriteString(s.String())
	}

	return out.String()
}

// ExpressionStatement is a group of expressions that allows us to group a
// subset of expressions.
type ExpressionStatement struct {
	Token      Token
	Expression Expression
}

// Pos returns the first position of the expression statement.
func (es *ExpressionStatement) Pos() Position {
	return es.Token.Pos
}

// End returns the last position of the expression statement.
func (es *ExpressionStatement) End() Position {
	return es.Expression.End()
}

func (es *ExpressionStatement) String() string {
	if es.Expression != nil {
		str := es.Expression.String()
		if str == "" {
			return ";"
		}
		if str[len(str)-1:] != " " {
			str += " "
		}
		return strings.TrimSpace(str)
	}
	return ""
}

// AccessorExpression represents an expression that is associated with an operator.
type AccessorExpression struct {
	Token Token
	Right Expression
	Left  Expression
}

// Pos returns the first position of the identifier.
func (ie *AccessorExpression) Pos() Position {
	return ie.Token.Pos
}

// End returns the last position of the identifier.
func (ie *AccessorExpression) End() Position {
	return ie.Right.End()
}

func (ie *AccessorExpression) String() string {
	var out bytes.Buffer

	out.WriteString(ie.Left.String())
	out.WriteString(".")
	out.WriteString(ie.Right.String())

	return out.String()
}

// IndexExpression represents an expression that is associated with an operator.
type IndexExpression struct {
	Token    Token
	Operator string
	Left     Expression
	Index    Expression
}

// Pos returns the first position of the identifier.
func (ie *IndexExpression) Pos() Position {
	return ie.Token.Pos
}

// End returns the last position of the identifier.
func (ie *IndexExpression) End() Position {
	return ie.Index.End()
}

func (ie *IndexExpression) String() string {
	var out bytes.Buffer

	out.WriteString("(")
	out.WriteString(ie.Left.String())
	out.WriteString("[")
	out.WriteString(ie.Index.String())
	out.WriteString("]")
	out.WriteString(")")

	return out.String()
}

// Identifier represents an identifier for a given AST block
type Identifier struct {
	Token Token
}

// Pos returns the first position of the identifier.
func (i *Identifier) Pos() Position {
	return i.Token.Pos
}

// End returns the last position of the identifier.
func (i *Identifier) End() Position {
	length := utf8.RuneCountInString(i.Token.Literal)
	return Position{
		Line:   i.Token.Pos.Line,
		Column: i.Token.Pos.Column + length,
	}
}

func (i *Identifier) String() string { return i.Token.Literal }

// String represents an string for a given AST block
type String struct {
	Token Token
}

// Pos returns the first position of the string.
func (i *String) Pos() Position {
	return i.Token.Pos
}

// End returns the last position of the string.
func (i *String) End() Position {
	length := utf8.RuneCountInString(i.Token.Literal)
	return Position{
		Line:   i.Token.Pos.Line,
		Column: i.Token.Pos.Column + length,
	}
}

func (i *String) String() string { return fmt.Sprintf("%q", i.Token.Literal) }

// Separator represents an separator expression
type Separator struct {
	Token Token
}

// Pos returns the first position of the separator expression.
func (i *Separator) Pos() Position {
	return i.Token.Pos
}

// End returns the last position of the separator expression.
func (i *Separator) End() Position {
	return i.Token.Pos
}

func (i *Separator) String() string { return " " }

// Empty represents an empty expression
type Empty struct {
	Token Token
}

// Pos returns the first position of the empty expression.
func (i *Empty) Pos() Position {
	return i.Token.Pos
}

// End returns the last position of the empty expression.
func (i *Empty) End() Position {
	return i.Token.Pos
}

func (i *Empty) String() string { return "()" }

// Integer represents an integer for a given AST block
type Integer struct {
	Token Token
	Value int64
}

// Pos returns the first position of the integer.
func (i *Integer) Pos() Position {
	return i.Token.Pos
}

// End returns the last position of the integer.
func (i *Integer) End() Position {
	length := utf8.RuneCountInString(i.Token.Literal)
	return Position{
		Line:   i.Token.Pos.Line,
		Column: i.Token.Pos.Column + length,
	}
}

func (i *Integer) String() string { return i.Token.Literal }
