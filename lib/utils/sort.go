package utils

import "sort"

// Sortable is the type of data to sort
type Sortable interface {
	Integer | Float | String
}

type Integer interface {
	Signed | Unsigned
}

type Signed interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64
}

type Unsigned interface {
	~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~uintptr
}

type Float interface {
	~float32 | ~float64
}

type String interface {
	~string
}

// Sort sorts a slice of Sortable in increasing order.
func Sort[T Sortable](data []T) {
	sort.Slice(data, func(i, j int) bool {
		return data[i] < data[j]
	})
}

func OrderSort[T Sortable](data []T, reverse bool) {
	sort.Slice(data, func(i, j int) bool {
		if reverse {
			return data[i] > data[j]
		}
		return data[i] < data[j]
	})
}

// Search searches for x in a sorted slice of Sortable.
//
// data must be sorted in increasing order.
//
// Return the index of the first element in data that is >= x.
// if there is no such element, return len(data).
func Search[T Sortable](data []T, x T) int {
	return sort.Search(len(data), func(i int) bool { return data[i] >= x })
}
