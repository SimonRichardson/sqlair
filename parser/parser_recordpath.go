package parser

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	LOWEST = iota
	INDEX
)

var precedence = map[TokenType]int{
	LBRACKET: INDEX,
	PERIOD:   INDEX,
}

type RecordPathParser struct {
	lex *Lexer

	errors []string

	currentToken Token
	peekToken    Token

	prefix map[TokenType]PrefixFunc
	infix  map[TokenType]InfixFunc
}

// NewRecordPathParser creates a parser for consuming a lexer tokens.
func NewRecordPathParser(lex *Lexer) *RecordPathParser {
	p := &RecordPathParser{
		lex: lex,
	}
	p.prefix = map[TokenType]PrefixFunc{
		IDENT:  p.parseIdentifier,
		INT:    p.parseInteger,
		STRING: p.parseString,
		LPAREN: p.parseGroup,
	}
	p.infix = map[TokenType]InfixFunc{
		PERIOD:   p.parseAccessor,
		LBRACKET: p.parseIndex,
	}
	p.nextToken()
	p.nextToken()
	return p
}

// Run the parser to the end, which is either an EOF or an error.
func (p *RecordPathParser) Run() (*QueryExpression, error) {
	var exp QueryExpression
	for p.currentToken.Type != EOF {
		exp.Expressions = append(exp.Expressions, p.parseExpressionStatement())
		p.nextToken()
	}
	var err error
	if len(p.errors) > 0 {
		err = fmt.Errorf(strings.Join(p.errors, "\n"))
		return nil, err
	}
	return &exp, nil
}

func (p *RecordPathParser) parseIdentifier() Expression {
	return &Identifier{
		Token: p.currentToken,
	}
}

func (p *RecordPathParser) parseString() Expression {
	return &String{
		Token: p.currentToken,
	}
}

func (p *RecordPathParser) parseInteger() Expression {
	value, err := strconv.ParseInt(p.currentToken.Literal, 10, 64)
	if err != nil {
		msg := fmt.Sprintf("Syntax Error:%v could not parse %q as integer", p.currentToken.Pos, p.currentToken.Literal)
		p.errors = append(p.errors, msg)
	}
	return &Integer{
		Token: p.currentToken,
		Value: value,
	}
}

func (p *RecordPathParser) parseExpressionStatement() Expression {
	stmt := &ExpressionStatement{
		Token: p.currentToken,
	}

	stmt.Expression = p.parseExpression(LOWEST)

	if p.isPeekToken(SEMICOLON) {
		p.nextToken()
	}
	return stmt
}

func (p *RecordPathParser) parseExpression(precedence int) Expression {
	prefix := p.prefix[p.currentToken.Type]
	if prefix == nil {
		if p.currentToken.Type != EOF {
			msg := fmt.Sprintf("Syntax Error:%v invalid character '%s' found", p.currentToken.Pos, p.currentToken.Type)
			p.errors = append(p.errors, msg)
		}
		return nil
	}
	leftExp := prefix()

	// Run the infix function until the next token has
	// a higher precedence.
	for !p.isPeekToken(SEMICOLON) && precedence < p.peekPrecedence() {
		infix := p.infix[p.peekToken.Type]
		if infix == nil {
			return leftExp
		}
		p.nextToken()
		leftExp = infix(leftExp)
	}

	return leftExp
}

func (p *RecordPathParser) parseGroup() Expression {
	p.nextToken()
	if p.currentToken.Type == LPAREN && p.isCurrentToken(RPAREN) {
		// This is an empty group, not sure what we should do here.
		return &Empty{
			Token: p.currentToken,
		}
	}

	exp := p.parseExpression(LOWEST)
	if !p.expectPeek(RPAREN) {
		return nil
	}
	return exp
}

func (p *RecordPathParser) parseIndex(left Expression) Expression {
	p.nextToken()

	expression := &IndexExpression{
		Token: p.currentToken,
		Left:  left,
		Index: p.parseExpression(LOWEST),
	}
	if !p.expectPeek(RBRACKET) {
		return nil
	}
	return expression
}

func (p *RecordPathParser) parseAccessor(left Expression) Expression {
	precedence := p.currentPrecedence()
	p.nextToken()
	right := p.parseExpression(precedence)

	return &AccessorExpression{
		Token: p.currentToken,
		Left:  left,
		Right: right,
	}
}

func (p *RecordPathParser) currentPrecedence() int {
	if p, ok := precedence[p.currentToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *RecordPathParser) peekPrecedence() int {
	if p, ok := precedence[p.peekToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *RecordPathParser) isPeekToken(t TokenType) bool {
	return p.peekToken.Type == t
}

func (p *RecordPathParser) isCurrentToken(t TokenType) bool {
	return p.currentToken.Type == t
}

func (p *RecordPathParser) nextToken() {
	p.currentToken = p.peekToken
	p.peekToken = p.lex.NextToken()
}

func (p *RecordPathParser) expectPeek(t TokenType) bool {
	if p.isPeekToken(t) {
		p.nextToken()
		return true
	}
	msg := fmt.Sprintf("Syntax Error: %v expected token to be %s, got %s instead", p.currentToken.Pos, t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
	return false
}
