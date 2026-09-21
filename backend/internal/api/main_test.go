package api_test

import (
	"testing"

	"ratelimit-gateway/internal/testboot"
)

func TestMain(m *testing.M) {
	testboot.Main(m, 55462, 12)
}
