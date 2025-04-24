module tp-sistemas-distribuidos/server

go 1.23.1

require github.com/rabbitmq/amqp091-go v1.10.0

require pkg v0.0.0

require (
	github.com/cdipaolo/goml v0.0.0-20220715001353-00e0c845ae1c // indirect
	github.com/cdipaolo/sentiment v0.0.0-20200617002423-c697f64e7f10 // indirect
	golang.org/x/text v0.3.6 // indirect
)

replace pkg => ../pkg
