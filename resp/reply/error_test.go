package reply

import (
	"fmt"
	"testing"
)

func TestNewWrongTypeErrReply(t *testing.T) {
	b := []byte("hello")
	fmt.Printf("%s\n", b)
}
