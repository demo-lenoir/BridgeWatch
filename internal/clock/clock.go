package clock

import "time"

// Clock supplies wall time at durable business decisions.
type Clock interface{ Now() time.Time }

type Real struct{}

func (Real) Now() time.Time { return time.Now().UTC() }
