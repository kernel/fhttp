package http2

import "time"

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
	// ActiveStreamsAtStart is sampled before attempting the PING.
	ActiveStreamsAtStart int
	// ActiveStreamsAtFailure is sampled before closing the failed connection.
	ActiveStreamsAtFailure int
}
