// Copyright (c) Microsoft. All rights reserved.
// Licensed under the MIT license. See LICENSE file in the project root for details.
package main

import (
	"time"
)

type MockClock struct {
	now               time.Time
	LastSleepDuration time.Duration
}

var _ Clock = (*MockClock)(nil) // ensure MockClock implements Clock

// Returns current time
func (mc *MockClock) Now() time.Time {
	return mc.now
}

// After waits for the duration to elapse and then sends the current time
// on the returned channel
func (mc *MockClock) After(d time.Duration) <-chan time.Time {
	mc.LastSleepDuration = d
	c := make(chan time.Time, 1)
	c <- mc.now
	return c
}

// Tells mock clock about time progression
func (mc *MockClock) NotifyTimeElapsed(d time.Duration) {
	mc.now = mc.Now().Add(d)
}
