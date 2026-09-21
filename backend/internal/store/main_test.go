package store_test

import (
	"testing"

	"ratelimit-gateway/internal/testboot"
)

func TestMain(m *testing.M) {
	testboot.Main(m, 55461, 11)
}
