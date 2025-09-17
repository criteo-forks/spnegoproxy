package spnegoproxy

import (
	"io"
	"net/http"
)

func ExposeMetrics(listenAddr string, events WebHDFSEventChannel) {
	srv := http.NewServeMux()
	logger.Printf("Configuring handlers for metrics on %s\n", listenAddr)
	srv.HandleFunc("/metrics", handleMetrics)
	srv.HandleFunc("/metrics/", handleMetrics)
	srv.HandleFunc("/", handleRoot)
	go serveMetrics(listenAddr, srv)
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "use /metrics")
}

func handleMetrics(w http.ResponseWriter, r *http.Request) {
	// ctx := r.Context()
	logger.Print("Requested metrics")
	io.WriteString(w, webHDFSEvents.String())
	io.WriteString(w, performanceCountersTable.String())

}

func serveMetrics(addr string, server *http.ServeMux) {
	logger.Printf("Serving metrics on %s\n", addr)
	http.ListenAndServe(addr, server)
}
