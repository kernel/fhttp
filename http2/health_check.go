package http2

import (
	"errors"
	"time"
)

const lostPingMessage = "http2: client connection lost"

// PingAckTimeoutError is returned to streams interrupted by a health check
// whose PING was flushed but not acknowledged before its deadline. Deadline
// belongs to the connection's PING, not to any individual request.
type PingAckTimeoutError struct {
	Deadline time.Time
}

func (PingAckTimeoutError) Error() string { return lostPingMessage }

// HealthCheckFailureDetails describes a failed health-check PING. Timings are
// retrospective and do not impose deadlines on the write lock or socket writes.
// Stream counts are snapshots, not a count of requests interrupted by the check.
type HealthCheckFailureDetails struct {
	// Phase is prepare, write_frame, flush, or ack_wait at the point of failure.
	Phase string
	// WriteLockWait is time spent acquiring the connection's write mutex.
	WriteLockWait time.Duration
	// WriteDuration includes writing the PING frame and flushing the TLS stream.
	WriteDuration time.Duration
	// AckWait is time spent waiting after a successful flush.
	AckWait time.Duration
	// TotalDuration includes all PING phases.
	TotalDuration time.Duration
	// PingWriteCompleted reports whether the PING flush succeeded.
	PingWriteCompleted bool
	// DeadlineExpiredBeforeAckWait distinguishes a deadline consumed before
	// waiting for the ACK from one that expires during the wait.
	DeadlineExpiredBeforeAckWait bool
	// AckTimeoutAt is the PING deadline if it expired while waiting for the
	// ACK, or zero for other failures, including a timeout on the reader.
	AckTimeoutAt time.Time
	// ActiveStreamsAtStart is sampled before attempting the PING.
	ActiveStreamsAtStart int
	// ActiveStreamsAtFailure is sampled before closing the failed connection.
	ActiveStreamsAtFailure int
}

func (d HealthCheckFailureDetails) streamError() error {
	if !d.AckTimeoutAt.IsZero() {
		return PingAckTimeoutError{Deadline: d.AckTimeoutAt}
	}
	return errors.New(lostPingMessage)
}
