FROM golang:latest

WORKDIR /src
COPY cmd ./cmd/
COPY spnegoproxy ./spnegoproxy/
WORKDIR /src/cmd/consulspnegoproxy
RUN go mod tidy
RUN grep "replace github.com/matchaxnb/spnego" go.mod || "replace %s => ../../spnegoproxy" "$(egrep -o 'github.com/matchaxnb/spnegoproxy/spnegoproxy v.*' go.mod)" | tee -a go.mod
RUN go build -o /spnego-proxy -ldflags '-linkmode external -extldflags "-fno-PIC -static"' .
FROM alpine:latest
WORKDIR /data
COPY --from=0 /spnego-proxy /spnego-proxy
ENV LISTEN_ADDRESS="0.0.0.0:50070" KRB5_CONF="/data/krb5.conf" \
    KRB5_KEYTAB="/data/krb5.keytab" KRB5_REALM="YOUR.REALM" \
    KRB5_USER="youruser/your.host" \
    CONSUL_ADDRESS="your.consul.address" \
    CONSUL_SERVICE_TO_PROXY="your-consul-service" \
    SPN_SERVICE_TYPE="HTTP" APP_DEBUG="false" \
    METRICS_ADDRESS="0.0.0.0:9100" PROPER_USER_NAME="" \
    DROP_USER_NAME="false"
SHELL [ "/bin/sh", "-c"]
EXPOSE 50070
COPY startup.sh /startup.sh
ENTRYPOINT [ "/startup.sh"]
