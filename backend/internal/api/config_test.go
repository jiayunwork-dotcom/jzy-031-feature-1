package api_test

import "ratelimit-gateway/internal/config"

func testCfg() config.Config {
	return config.Config{HTTPAddr: ":0"}
}
