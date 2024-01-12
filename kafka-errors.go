package kafkamock

type (
	kafkaErrorCode int16
)

const (
	Unknown kafkaErrorCode = iota - 1
	NoError
	UnknownTopicOrPartition
)