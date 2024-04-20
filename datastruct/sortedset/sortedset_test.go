package sortedset

import "testing"

func TestSortedSet_Add(t *testing.T) {
	set := NewSortedSet()
	set.Add("jack", 100)
	set.Add("lily", 90)
	set.Add("quin", 80)
	if set.Length() != 3 {
		t.Errorf("expect 3, but got %d", set.Length())
	} else {
		elements := set.RangeByRank(0, 3, false)
		if len(elements) != 3 {
			t.Errorf("expect 3, but got %d", len(elements))
		}
		for _, e := range elements {
			t.Log("element:", e)
		}
	}
}

func TestSortedSet_PopMax(t *testing.T) {
	set := NewSortedSet()
	set.Add("jack", 100)
	set.Add("lily", 90)
	set.Add("quin", 80)
	elements := set.PopMax(2)
	for _, e := range elements {
		t.Log("element:", e)
	}
}

func TestSortedSet_RangeByRank(t *testing.T) {
	set := NewSortedSet()
	set.Add("jack", 100)
	set.Add("lily", 90)
	set.Add("quin", 80)
	elements := set.RangeByRank(0, 2, false)
	for _, e := range elements {
		t.Log("element:", e)
	}
}

func TestSortedSet_RemoveByRank(t *testing.T) {
	set := NewSortedSet()
	set.Add("jack", 100)
	set.Add("lily", 90)
	set.Add("quin", 80)
	n := set.RemoveByRank(0, 1)
	if n != 2 {
		t.Errorf("expect 2, but got %d", n)
	} else {
		t.Log("set.Length():", set.Length())
	}
}

func TestSortedSet_GetRank(t *testing.T) {
	set := NewSortedSet()
	set.Add("jack", 100)
	set.Add("lily", 90)
	set.Add("quin", 80)
	rank := set.GetRank("jack", false)
	t.Log("rank:", rank)
}

func TestSortedSet_Remove(t *testing.T) {
	set := NewSortedSet()
	set.Add("jack", 100)
	set.Add("lily", 90)
	set.Add("quin", 80)
	if set.Remove("jack") {
		t.Log("remove success")
		count := set.RangeCount(scoreNegativeInfBorder, scorePositiveInfBorder)
		t.Log("count:", count)
	}
}
