package cache

import (
	"context"
	pb "goCache/groupcachepb"
)

type PeerPicker interface {
	PickPeer(key string) (peer PeerGetter, ok bool)
}

type PeerGetter interface {
	Get(ctx context.Context, in *pb.Request, out *pb.Response) error
}

type PeerIncrementer interface {
	Increment(ctx context.Context, in *pb.Request, out *pb.Response) error
}

type PeerInvalidator interface {
	Invalidate(in *pb.Request) error
}

type PeerInvalidationBroadcaster interface {
	BroadcastInvalidate(in *pb.Request) error
}
