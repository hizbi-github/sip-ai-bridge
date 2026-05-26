package rb

import "errors"

type RingBuffer[T any] struct {
	Data       []T
	WriteIndex int
	ReadIndex  int
	Size       int
}

func New[T any](size int) RingBuffer[T] {
	return RingBuffer[T]{
		Data:       make([]T, size),
		WriteIndex: 0,
		ReadIndex:  0,
		Size:       size,
	}
}

func Push[T any](elem T, buffer *RingBuffer[T]) {
	buffer.Data[buffer.WriteIndex] = elem

	buffer.WriteIndex = buffer.WriteIndex + 1
	if buffer.WriteIndex >= buffer.Size {
		buffer.WriteIndex = 0
	}
}

func Pop[T any](buffer *RingBuffer[T]) (T, error) {
	var elem T

	if buffer.ReadIndex == buffer.WriteIndex {
		return elem, errors.New("ring buffer is empty")
	}

	elem = buffer.Data[buffer.ReadIndex]
	buffer.ReadIndex = buffer.ReadIndex + 1
	if buffer.ReadIndex >= buffer.Size {
		buffer.ReadIndex = 0
	}

	return elem, nil
}
