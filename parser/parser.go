package parser

import (
	"fmt"
	"strconv"
	"strings"
)

type PrefixFunc func() Expression
type InfixFunc func(Expression) Expression

const (
	LOWEST = iota
	INDEX
)

var precedence = map[TokenType]int{
	LBRACKET: INDEX,
	PERIOD:   INDEX,
}

type Parser struct {
	lex *Lexer

	errors []string

	currentToken Token
	peekToken    Token

	prefix map[TokenType]PrefixFunc
	infix  map[TokenType]InfixFunc

	terminated bool
}

// NewParser creates a parser for consuming a lexer tokens.
func NewParser(lex *Lexer) *Parser {
	p := &Parser{
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
	p.lex.ReadNext()
	p.nextToken()
	p.nextToken()
	return p
}

// Run the parser to the end, which is either an EOF or an error.
func (p *Parser) Run() (*QueryExpression, int, error) {
	var exp QueryExpression
	for p.currentToken.Type != EOF {
		if p.terminated {
			break
		}
		exp.Expressions = append(exp.Expressions, p.parseExpressionStatement())
		p.nextToken()
	}
	var err error
	if len(p.errors) > 0 {
		err = fmt.Errorf(strings.Join(p.errors, "\n"))
		return nil, p.lex.position, err
	}
	return &exp, p.currentToken.Pos.Offset, nil
}

func (p *Parser) parseIdentifier() Expression {
	return &Identifier{
		Token: p.currentToken,
	}
}

func (p *Parser) parseString() Expression {
	return &String{
		Token: p.currentToken,
	}
}

func (p *Parser) parseInteger() Expression {
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

func (p *Parser) parseExpressionStatement() Expression {
	stmt := &ExpressionStatement{
		Token: p.currentToken,
	}

	stmt.Expression = p.parseExpression(LOWEST)

	if p.isPeekToken(SEPARATOR) {
		p.terminated = true
		return stmt
	}
	return stmt
}

func (p *Parser) parseExpression(precedence int) Expression {
	prefix := p.prefix[p.currentToken.Type]
	if prefix == nil {
		if p.terminated {
			return nil
		}
		if p.currentToken.Type != EOF {
			msg := fmt.Sprintf("Syntax Error:%v invalid character '%s' found", p.currentToken.Pos, p.currentToken.Type)
			p.errors = append(p.errors, msg)
		}
		return nil
	}
	leftExp := prefix()

	// Run the infix function until the next token has
	// a higher precedence.
	for !p.isPeekToken(SEPARATOR) && precedence < p.peekPrecedence() {
		infix := p.infix[p.peekToken.Type]
		if infix == nil {
			return leftExp
		}
		p.nextToken()
		leftExp = infix(leftExp)
	}

	return leftExp
}

func (p *Parser) parseGroup() Expression {
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

func (p *Parser) parseIndex(left Expression) Expression {
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

func (p *Parser) parseAccessor(left Expression) Expression {
	precedence := p.currentPrecedence()
	p.nextToken()
	right := p.parseExpression(precedence)

	return &AccessorExpression{
		Token: p.currentToken,
		Left:  left,
		Right: right,
	}
}

func (p *Parser) currentPrecedence() int {
	if p, ok := precedence[p.currentToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) peekPrecedence() int {
	if p, ok := precedence[p.peekToken.Type]; ok {
		return p
	}
	return LOWEST
}

func (p *Parser) isPeekToken(t TokenType) bool {
	return p.peekToken.Type == t
}

func (p *Parser) isCurrentToken(t TokenType) bool {
	return p.currentToken.Type == t
}

func (p *Parser) nextToken() {
	p.currentToken = p.peekToken
	p.peekToken = p.lex.NextToken()
}

func (p *Parser) expectPeek(t TokenType) bool {
	if p.isPeekToken(t) {
		p.nextToken()
		return true
	}
	msg := fmt.Sprintf("Syntax Error: %v expected token to be %s, got %s instead", p.currentToken.Pos, t, p.peekToken.Type)
	p.errors = append(p.errors, msg)
	return false
}
