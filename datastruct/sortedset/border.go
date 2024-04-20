package sortedset

import (
	"math"
	"strconv"

	"go-redis/enum"
)

/*
 * ScoreBorder is a struct represents `min` `max` parameter of redis command `ZRANGEBYSCORE`
 * can accept:
 *   int or float value, such as 2.718, 2, -2.718, -2 ...
 *   exclusive int or float value, such as (2.718, (2, (-2.718, (-2 ...
 *   infinity: +inf, -inf， inf(same as +inf)
 */

const (
	ScoreNegativeInf int8 = -1 // score的下边界
	ScorePositiveInf int8 = 1  // score的上边界
	lexNegativeInf   int8 = '-'
	lexPositiveInf   int8 = '+'
)

type Border interface {
	greater(e *Element) bool          // receiver是否大于e
	less(e *Element) bool             // receiver是否小于e
	getValue() any                    // 获取receiver的值
	getExclude() bool                 // 获取receiver的开闭情况
	isNotIntersected(max Border) bool // 判断receiver和max是否有交集
}

// ScoreBorder represents range of a float value, including: <, <=, >, >=, +inf, -inf
type ScoreBorder struct {
	inf     int8
	value   float64
	exclude bool
}

// if max.greater(score) then the score is within the upper border
// do not use min.greater()
func (border *ScoreBorder) greater(e *Element) bool {
	value := e.Score
	if border.inf == ScoreNegativeInf {
		return false
	} else if border.inf == ScorePositiveInf {
		return true
	}
	if border.exclude {
		return border.value > value
	}
	return border.value >= value
}

func (border *ScoreBorder) less(e *Element) bool {
	value := e.Score
	if border.inf == ScoreNegativeInf {
		return true
	} else if border.inf == ScorePositiveInf {
		return false
	}
	if border.exclude {
		return border.value < value
	}
	return border.value <= value
}

func (border *ScoreBorder) getValue() interface{} {
	return border.value
}

func (border *ScoreBorder) getExclude() bool {
	return border.exclude
}

var scorePositiveInfBorder = &ScoreBorder{
	inf:   ScorePositiveInf,
	value: math.Inf(1),
}

var scoreNegativeInfBorder = &ScoreBorder{
	inf:   ScoreNegativeInf,
	value: math.Inf(1),
}

// ParseScoreBorder creates ScoreBorder from redis arguments
func ParseScoreBorder(s string) (Border, error) {
	// 1. 处理字符串
	if s == "inf" || s == "+inf" {
		return scorePositiveInfBorder, nil
	}
	if s == "-inf" {
		return scoreNegativeInfBorder, nil
	}
	// 2. 开区间
	if s[0] == '(' {
		value, err := strconv.ParseFloat(s[1:], 64)
		if err != nil {
			return nil, enum.MIN_OR_MAX_IS_NOT_A_FLOAT
		}
		return &ScoreBorder{
			inf:     0,
			value:   value,
			exclude: true,
		}, nil
	}
	// 3. 闭区间
	value, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil, enum.MIN_OR_MAX_IS_NOT_A_FLOAT
	}
	return &ScoreBorder{
		inf:     0,
		value:   value,
		exclude: false,
	}, nil
}

func (border *ScoreBorder) isNotIntersected(max Border) bool {
	minValue := border.value
	maxValue := max.(*ScoreBorder).value
	return minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}

// LexBorder represents range of a string value, including: <, <=, >, >=, +, -
type LexBorder struct {
	Inf     int8
	Value   string
	Exclude bool
}

// if max.greater(lex) then the lex is within the upper border
// do not use min.greater()
func (border *LexBorder) greater(e *Element) bool {
	value := e.Ele
	if border.Inf == lexNegativeInf {
		return false
	} else if border.Inf == lexPositiveInf {
		return true
	}
	if border.Exclude {
		return border.Value > value
	}
	return border.Value >= value
}

func (border *LexBorder) less(e *Element) bool {
	value := e.Ele
	if border.Inf == lexNegativeInf {
		return true
	} else if border.Inf == lexPositiveInf {
		return false
	}
	if border.Exclude {
		return border.Value < value
	}
	return border.Value <= value
}

func (border *LexBorder) getValue() interface{} {
	return border.Value
}

func (border *LexBorder) getExclude() bool {
	return border.Exclude
}

var lexPositiveInfBorder = &LexBorder{
	Inf: lexPositiveInf,
}

var lexNegativeInfBorder = &LexBorder{
	Inf: lexNegativeInf,
}

// ParseLexBorder creates LexBorder from redis arguments
func ParseLexBorder(s string) (Border, error) {
	if s == "+" {
		return lexPositiveInfBorder, nil
	}
	if s == "-" {
		return lexNegativeInfBorder, nil
	}
	if s[0] == '(' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: true,
		}, nil
	}

	if s[0] == '[' {
		return &LexBorder{
			Inf:     0,
			Value:   s[1:],
			Exclude: false,
		}, nil
	}

	return nil, enum.MIN_OR_MAX_IS_NOT_VALID_STRING
}

func (border *LexBorder) isNotIntersected(max Border) bool {
	minValue := border.Value
	maxValue := max.(*LexBorder).Value
	return border.Inf == '+' || minValue > maxValue || (minValue == maxValue && (border.getExclude() || max.getExclude()))
}
