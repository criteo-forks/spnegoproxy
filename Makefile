.PHONY = (all test clean plainproxydkr)
all: plain-proxy consul-proxy fixed-target-proxy

clean:
	rm -rf go.mod go.sum plain-proxy consul-proxy fixed-target-proxy
	go mod init app
	go mod tidy

plain-proxy: spnegoproxy/*.go cmd/plainproxy/*.go
	cd cmd/plainproxy; \
	go build -o ../../plain-proxy main.go

plain-proxy-static: spnegoproxy/*.go cmd/plainproxy/*.go
	cd cmd/plainproxy; \
	CGO_ENABLED=0 go build -o ../../plain-proxy-static
	strip plain-proxy-static

consul-proxy: spnegoproxy/*.go cmd/consulspnegoproxy/*.go
	cd cmd/consulspnegoproxy; \
	go build -o ../../consul-proxy main.go

consul-proxy-static: spnegoproxy/*.go cmd/consulspnegoproxy/*.go
	cd cmd/consulspnegoproxy; \
	CGO_ENABLED=0 go build -o ../../consul-proxy-static main.go
	strip consul-proxy-static

fixed-target-proxy: spnegoproxy/*.go cmd/fixedtargetproxy/*.go
	cd cmd/fixedtargetproxy; \
	go build -o ../../fixed-target-proxy main.go

fixed-target-proxy-static: spnegoproxy/*.go cmd/fixedtargetproxy/*.go
	cd cmd/fixedtargetproxy; \
	CGO_ENABLED=0 go build -o ../../fixed-target-proxy-static main.go
	strip fixed-target-proxy-static

plainproxydkr:
	set -x
	docker build -t ppdkr:0.0.1 -f Dockerfile.plain .
	docker run --rm -ti --entrypoint /spnego-proxy --name plainproxy -p 50070:50070 ppdkr:0.0.1 -addr 0.0.0.0:50070 -proxy-service $(TO_PROXY)
