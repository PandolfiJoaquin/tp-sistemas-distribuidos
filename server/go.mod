module tp-sistemas-distribuidos/server

go 1.23.1

require github.com/rabbitmq/amqp091-go v1.10.0

require (
	github.com/cdipaolo/sentiment v0.0.0-20200617002423-c697f64e7f10
	github.com/stretchr/testify v1.7.0
	pkg v0.0.0
)

require (
	github.com/cdipaolo/goml v0.0.0-20220715001353-00e0c845ae1c // indirect
	github.com/davecgh/go-spew v1.1.0 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	golang.org/x/text v0.3.6 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)

replace pkg => ../pkg
