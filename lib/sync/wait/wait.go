package wait

import (
	"go-redis/lib/logger"
	"sync"
	"time"
)

// Wait is similar with sync.WaitGroup which can wait with timeout
type Wait struct {
	wg sync.WaitGroup
}

// Add adds delta, which may be negative, to the WaitGroup counter.
func (w *Wait) Add(delta int) {
	w.wg.Add(delta)
}

// Done decrements the WaitGroup counter by one
func (w *Wait) Done() {
	w.wg.Done()
}

// Wait blocks until the WaitGroup counter is zero.
func (w *Wait) Wait() {
	w.wg.Wait()
}

// WaitWithTimeout blocks until the WaitGroup counter is zero or timeout
// returns true if timeout
func (w *Wait) WaitWithTimeout(timeout time.Duration) bool {
	c := make(chan struct{})

	go func() {
		defer close(c)
		w.wg.Wait()
		c <- struct{}{}
	}()

	select {
	case <-c:
		return false // completed normally
	case <-time.After(timeout):
		logger.Error("wait timeout")
		return true // timed out
	}
}
