package parser

import "fmt"

// TokenType represents a way to identify an individual token.
type TokenType int

const (
	UNKNOWN TokenType = (iota - 1)
	EOF
	SEPARATOR

	IDENT
	INT //int literal
	STRING

	COMMA // ,

	LPAREN   // (
	RPAREN   // )
	LBRACKET // [
	RBRACKET // ]

	BITAND // &
	PERIOD // .
)

func (t TokenType) String() string {
	switch t {
	case EOF:
		return "EOF"
	case IDENT:
		return "IDENT"
	case INT:
		return "INT"
	case PERIOD:
		return "."
	case COMMA:
		return ","
	case LPAREN:
		return "("
	case RPAREN:
		return ")"
	case LBRACKET:
		return "["
	case RBRACKET:
		return "]"
	case BITAND:
		return "&"
	case STRING:
		return `""`
	case SEPARATOR:
		return ` `
	default:
		return "<UNKNOWN>"
	}
}

// Position holds the location of the token within the query.
type Position struct {
	Offset int
	Line   int
	Column int
}

func (p Position) String() string {
	return fmt.Sprintf("<:%d:%d>", p.Line, p.Column)
}

// Token defines a token found with in a query, along with the position and what
// type it is.
type Token struct {
	Pos     Position
	Type    TokenType
	Literal string
}

// MakeToken creates a new token value.
func MakeToken(tokenType TokenType, char rune) Token {
	return Token{
		Type:    tokenType,
		Literal: string(char),
	}
}

var (
	// UnknownToken can be used as a sentinel token for a unknown state.
	UnknownToken = Token{
		Type: UNKNOWN,
	}
)

var tokenMap = map[rune]TokenType{
	'(': LPAREN,
	')': RPAREN,
	'[': LBRACKET,
	']': RBRACKET,
	',': COMMA,
	'&': BITAND,
	'.': PERIOD,
}
