PROTO_ROOT := proto
PROTO_FILES := $(shell find $(PROTO_ROOT) -name '*.proto')
GO_GEN_OUT := server/internal/gen
TS_GEN_OUT := client/gen
TS_PROTO_PLUGIN := client/node_modules/.bin/protoc-gen-ts_proto

.PHONY: generate generate-go generate-ts run client
generate: generate-go generate-ts

generate-go:
	mkdir -p $(GO_GEN_OUT)
	protoc \
		-I $(PROTO_ROOT) \
		--go_out=$(GO_GEN_OUT) --go_opt=paths=source_relative \
		--go-grpc_out=$(GO_GEN_OUT) --go-grpc_opt=paths=source_relative \
		$(PROTO_FILES)

$(TS_PROTO_PLUGIN):
	cd client && npm install

generate-ts: $(TS_PROTO_PLUGIN)
	mkdir -p $(TS_GEN_OUT)
	protoc -I $(PROTO_ROOT) \
		--plugin=protoc-gen-ts_proto=$(TS_PROTO_PLUGIN) \
		--ts_proto_out=$(TS_GEN_OUT) \
		--ts_proto_opt=outputServices=default,useAsyncIterable=true,esmInterop=true,importSuffix=.js,useOptionals=messages \
		$(PROTO_FILES)

run:
	docker-compose up --build --abort-on-container-exit

client:
	cd ../../client-web && npm run build
	cd client && npm install && npm start
