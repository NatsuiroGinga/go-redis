package timewheel

import (
	"time"

	"go-redis/config"
)

var timeWheel *TimeWheel

const SLOW_NUM = 360

func init() {
	duration := time.Duration(config.Properties.Cycle) * time.Second
	timeWheel = NewTimeWheel(duration, SLOW_NUM)
	timeWheel.Start()
}

// Delay executes job after waiting the given duration
func Delay(duration time.Duration, key string, job func()) {
	timeWheel.AddJob(duration, key, job)
}

// At executes job at given time
func At(at time.Time, key string, job func()) {
	timeWheel.AddJob(at.Sub(time.Now()), key, job)
}

// Cancel stops a pending job
func Cancel(key string) {
	timeWheel.RemoveJob(key)
}
