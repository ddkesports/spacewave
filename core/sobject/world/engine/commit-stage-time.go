package sobject_world_engine

import "time"

// CommitStageTime is the duration of one stage of a World write.
type CommitStageTime struct {
	// Name is the stage's log name, such as lock-wait or receipt-wait.
	Name string
	// Duration is the time the write spent in the stage.
	Duration time.Duration
}
