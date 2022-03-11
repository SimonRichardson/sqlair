package parser

type PrefixFunc func() Expression
type InfixFunc func(Expression) Expression
