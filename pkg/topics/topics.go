package topics

type Topics string

const (
	ORDERS_CREATED Topics = "orders_created"
	RETRY_5M       Topics = "retry_5m"
	RETRY_20M      Topics = "retry_20m"
	RETRY_40M      Topics = "retry_40m"
	ORDERS_DLQ     Topics = "orders_dlq"
)

func (t Topics) String() string {
	return string(t)
}

type ConsumerGroup string

const (
	ORDERS_MAIN_GROUP      ConsumerGroup = "orders_main_group"
	ORDERS_5M_RETRY_GROUP  ConsumerGroup = "orders_retry_5m_group"
	ORDERS_20M_RETRY_GROUP ConsumerGroup = "orders_retry_20m_group"
	ORDERS_40M_RETRY_GROUP ConsumerGroup = "orders_retry_40m_group"
)

func (t ConsumerGroup) String() string {
	return string(t)
}
