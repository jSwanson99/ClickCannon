package app

import "context"

type Worker interface {
	ID() int
	Run(ctx context.Context) error
}
