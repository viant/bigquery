package matcher

import (
	"github.com/viant/parsly"
)

// Selector represent a Selector matcher
type Selector struct{}

// Match matches a string
func (n *Selector) Match(cursor *parsly.Cursor) (matched int) {
	input := cursor.Input
	pos := cursor.Pos
	size := len(input)
	if startsWithCharacter := IsLetter(input[pos]); startsWithCharacter {
		pos++
		matched++
	} else if input[pos] == '[' {
		pos++
		matched++
		for i := pos; i < size; i++ {
			pos++
			matched++
			if input[i] == ']' {
				return
			}
		}
		return 0
	} else if input[pos] == '`' {
		pos++
		matched++
		for i := pos; i < size; i++ {
			pos++
			matched++
			if input[i] == '`' {
				return
			}
		}
	} else {
		return 0
	}

	for i := pos; i < size; i++ {
		switch input[i] {
		case '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '_', '.', ':', '-':
			matched++
			continue
		default:
			if IsLetter(input[i]) {
				matched++
				continue
			}
			return matched
		}
	}

	return matched
}

// NewSelector creates a Selector matcher
func NewSelector() *Selector {
	return &Selector{}
}

// IsLetter checks if passed byte means a letter
func IsLetter(b byte) bool {
	if (b < 'a' || b > 'z') && (b < 'A' || b > 'Z') {
		return false
	}
	return true
}
