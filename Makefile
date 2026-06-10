.PHONY= run_consumer run_producer avro

run_consumer:
	go run consumer/main.go

run_producer:
	go run producer/main.go


avro:
	mkdir -p schemas/avro
	gogen-avro  -package avro schemas/avro schemas/user.avsc