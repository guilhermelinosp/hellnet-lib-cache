package cache

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestHandshakeCommandsAreNotTraced(t *testing.T) {
	ctx := context.Background()
	handshake := []redis.Cmder{
		redis.NewCmd(ctx, "hello", 3),
		redis.NewCmd(ctx, "client", "maint_notifications", "on"),
		redis.NewCmd(ctx, "CLIENT", "setinfo", "LIB-NAME", "go-redis"),
	}
	for _, cmd := range handshake {
		if !handshakeCommand(cmd) {
			t.Errorf("%s must be filtered from tracing", cmd.Name())
		}
	}
	for _, cmd := range []redis.Cmder{
		redis.NewCmd(ctx, "get", "k"),
		redis.NewCmd(ctx, "set", "k", "v", "ex", 10),
		redis.NewCmd(ctx, "del", "k"),
	} {
		if handshakeCommand(cmd) {
			t.Errorf("%s is data traffic and must stay traced", cmd.Name())
		}
	}
}

func TestHandshakePipelineOnlyWhenEveryCommandIsHandshake(t *testing.T) {
	ctx := context.Background()
	setinfo := []redis.Cmder{
		redis.NewCmd(ctx, "client", "setinfo", "LIB-NAME", "go-redis"),
		redis.NewCmd(ctx, "client", "setinfo", "LIB-VER", "9"),
	}
	if !handshakePipeline(setinfo) {
		t.Error("a pipeline of CLIENT SETINFO must be filtered")
	}
	mixed := append([]redis.Cmder{redis.NewCmd(ctx, "get", "k")}, setinfo...)
	if handshakePipeline(mixed) {
		t.Error("a pipeline with a data command must stay traced")
	}
	if handshakePipeline(nil) {
		t.Error("an empty pipeline is not a handshake")
	}
}
