package main

import (
	"context"

	"github.com/redis/go-redis/v9"
)

// skipEvalSha is a go-redis hook for Redis users whose ACL allows EVAL but not
// EVALSHA (like Laravel Cloud's). asynq runs its Lua scripts via redis.Script.Run,
// which tries EVALSHA first and falls back to EVAL on a NOSCRIPT error, so we
// answer every EVALSHA with NOSCRIPT without sending it to the server.
type skipEvalSha struct{}

type noScriptError struct{}

func (noScriptError) Error() string { return "NOSCRIPT EVALSHA skipped, use EVAL" }
func (noScriptError) RedisError()   {}

func (skipEvalSha) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (skipEvalSha) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func (skipEvalSha) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		switch cmd.Name() {
		case "evalsha", "evalsha_ro":
			cmd.SetErr(noScriptError{})
			return cmd.Err()
		}

		return next(ctx, cmd)
	}
}
