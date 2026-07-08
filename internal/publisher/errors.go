package publisher

import (
	"errors"

	"github.com/nats-io/nats.go"
)

// jsErrCodeMessageSizeExceedsMaximum is the JetStream API error code returned
// when a message exceeds the stream's max_msg_size. nats.go v1.47 does not
// define a named constant for it.
const jsErrCodeMessageSizeExceedsMaximum nats.ErrorCode = 10054

// IsPermanentPublishError reports whether a publish error can never succeed on
// retry for this specific message (a "poison message"). The allowlist is
// deliberately conservative: anything unrecognized is treated as transient so
// that infrastructure failures (timeouts, disconnects, auth misconfiguration)
// keep crashing the engine instead of skipping data.
func IsPermanentPublishError(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, nats.ErrMaxPayload) || errors.Is(err, nats.ErrBadSubject) {
		return true
	}
	var apiErr *nats.APIError
	if errors.As(err, &apiErr) {
		return apiErr.ErrorCode == jsErrCodeMessageSizeExceedsMaximum
	}
	return false
}
