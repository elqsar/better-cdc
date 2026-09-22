//go:build integration

package publisher

import "os"

// This hook is absent from release builds. It places a barrier at a precise
// durable-write boundary so the integration suite can terminate the process.
func init() {
	stage, path := os.Getenv("CDC_TEST_PAUSE_STAGE"), os.Getenv("CDC_TEST_STAGE_FILE")
	if stage == "" || path == "" {
		return
	}
	quarantineStage = func(current string) {
		if stage != current {
			return
		}
		if err := os.WriteFile(path, []byte(current), 0600); err != nil {
			panic(err)
		}
		select {}
	}
}
