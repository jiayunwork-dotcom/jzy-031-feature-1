package quota_test

import (
	"testing"

	"ratelimit-gateway/internal/testboot"
)

func TestMain(m *testing.M) {
	testboot.Main(m, 55460, 10)
}
