package common

type WriteAdapter interface {
	Write([]*RawLogEntity) error
	Close() error
}
