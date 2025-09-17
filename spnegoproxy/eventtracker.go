package spnegoproxy

import (
	"fmt"
	"net/http"
	"strings"
	"time"
)

// SPNEGOProxyWebHDFSEventsTable holds counters for each WebHDFS event
type SPNEGOProxyWebHDFSEventsTable struct {
	// when the app spawned
	tracking_time_start time.Time
	// HTTP GET operations
	get_open               int
	get_getfilestatus      int
	get_liststatus         int
	get_getcontentsummary  int
	get_getfilechecksum    int
	get_gethomedirectory   int
	get_getdelegationtoken int

	// HTTP PUT operations
	put_create                int
	put_mkdirs                int
	put_rename                int
	put_setreplication        int
	put_setowner              int
	put_setpermission         int
	put_settimes              int
	put_renewdelegationtoken  int
	put_canceldelegationtoken int

	// HTTP POST operations
	post_append int

	// HTTP DELETE operations
	delete_delete int

	// Invalid ops
	get_invalid    int
	put_invalid    int
	post_invalid   int
	delete_invalid int
}

// newHDFSEventTable initializes all event counters to zero
func newHDFSEventTable() *SPNEGOProxyWebHDFSEventsTable {
	return &SPNEGOProxyWebHDFSEventsTable{
		// HTTP GET operations
		get_open:               0,
		get_getfilestatus:      0,
		get_liststatus:         0,
		get_getcontentsummary:  0,
		get_getfilechecksum:    0,
		get_gethomedirectory:   0,
		get_getdelegationtoken: 0,

		// HTTP PUT operations
		put_create:                0,
		put_mkdirs:                0,
		put_rename:                0,
		put_setreplication:        0,
		put_setowner:              0,
		put_setpermission:         0,
		put_settimes:              0,
		put_renewdelegationtoken:  0,
		put_canceldelegationtoken: 0,

		// HTTP POST operations
		post_append: 0,

		// HTTP DELETE operations
		delete_delete: 0,

		// INVALID OPS
		get_invalid:    0,
		put_invalid:    0,
		post_invalid:   0,
		delete_invalid: 0,

		// uptime
		tracking_time_start: time.Now(),
	}
}

var webHDFSEvents = newHDFSEventTable()

func (events *SPNEGOProxyWebHDFSEventsTable) String() string {
	var sb strings.Builder

	// Iterate through each field in the struct
	sb.WriteString(fmt.Sprintf("webhdfs_get_open %d\n", events.get_open))
	sb.WriteString(fmt.Sprintf("webhdfs_get_getfilestatus %d\n", events.get_getfilestatus))
	sb.WriteString(fmt.Sprintf("webhdfs_get_liststatus %d\n", events.get_liststatus))
	sb.WriteString(fmt.Sprintf("webhdfs_get_getcontentsummary %d\n", events.get_getcontentsummary))
	sb.WriteString(fmt.Sprintf("webhdfs_get_getfilechecksum %d\n", events.get_getfilechecksum))
	sb.WriteString(fmt.Sprintf("webhdfs_get_gethomedirectory %d\n", events.get_gethomedirectory))
	sb.WriteString(fmt.Sprintf("webhdfs_get_getdelegationtoken %d\n", events.get_getdelegationtoken))
	sb.WriteString(fmt.Sprintf("webhdfs_get_total %d\n",
		events.get_open+
			events.get_getfilestatus+
			events.get_liststatus+
			events.get_getcontentsummary+
			events.get_getfilechecksum+
			events.get_gethomedirectory+
			events.get_getdelegationtoken+
			events.get_invalid))

	sb.WriteString(fmt.Sprintf("webhdfs_put_create %d\n", events.put_create))
	sb.WriteString(fmt.Sprintf("webhdfs_put_mkdirs %d\n", events.put_mkdirs))
	sb.WriteString(fmt.Sprintf("webhdfs_put_rename %d\n", events.put_rename))
	sb.WriteString(fmt.Sprintf("webhdfs_put_setreplication %d\n", events.put_setreplication))
	sb.WriteString(fmt.Sprintf("webhdfs_put_setowner %d\n", events.put_setowner))
	sb.WriteString(fmt.Sprintf("webhdfs_put_setpermission %d\n", events.put_setpermission))
	sb.WriteString(fmt.Sprintf("webhdfs_put_settimes %d\n", events.put_settimes))
	sb.WriteString(fmt.Sprintf("webhdfs_put_renewdelegationtoken %d\n", events.put_renewdelegationtoken))
	sb.WriteString(fmt.Sprintf("webhdfs_put_canceldelegationtoken %d\n", events.put_canceldelegationtoken))
	sb.WriteString(fmt.Sprintf("webhdfs_put_total %d\n",
		events.put_create+
			events.put_mkdirs+
			events.put_rename+
			events.put_setreplication+
			events.put_setowner+
			events.put_setpermission+
			events.put_settimes+
			events.put_renewdelegationtoken+
			events.put_canceldelegationtoken+
			events.put_invalid))

	sb.WriteString(fmt.Sprintf("webhdfs_post_append %d\n", events.post_append))
	sb.WriteString(fmt.Sprintf("webhdfs_post_total %d\n", events.post_append+events.post_invalid))

	sb.WriteString(fmt.Sprintf("webhdfs_delete_delete %d\n", events.delete_delete))
	sb.WriteString(fmt.Sprintf("webhdfs_delete_total %d\n", events.delete_delete+events.delete_invalid))
	// handle uptime
	uptime := int(time.Since(events.tracking_time_start).Seconds())
	sb.WriteString(fmt.Sprintf("proxy_start_timestamp %d\n", events.tracking_time_start.Unix()))
	sb.WriteString(fmt.Sprintf("proxy_current_time %d\n", time.Now().Unix()))
	sb.WriteString(fmt.Sprintf("proxy_uptime %d\n", uptime))
	return sb.String()
}

type PerformanceCountersTable struct {
	requests_processed             int64
	responses_processed            int64
	loglines_processed             int64
	average_request_processing_us  int64
	average_response_processing_us int64
	average_logger_processing_us   int64
}

func newPerformanceCountersTable() *PerformanceCountersTable {
	return &PerformanceCountersTable{
		requests_processed:             0,
		responses_processed:            0,
		loglines_processed:             0,
		average_request_processing_us:  0,
		average_response_processing_us: 0,
		average_logger_processing_us:   0,
	}
}

func updateRequestsPerformanceCounters(startTime time.Time) {
	dur := time.Since(startTime)
	performanceCountersTable.average_request_processing_us += dur.Microseconds()
	performanceCountersTable.average_request_processing_us >>= 1 // the wise girl's /= 2
	performanceCountersTable.requests_processed += 1
}

func updateResponsesPerformanceCounters(startTime time.Time) {
	dur := time.Since(startTime)
	performanceCountersTable.average_response_processing_us += dur.Microseconds()
	performanceCountersTable.average_response_processing_us >>= 1
	performanceCountersTable.responses_processed += 1
}

func updateLoggerPerformanceCounters(startTime time.Time) {
	dur := time.Since(startTime)
	performanceCountersTable.loglines_processed += 1
	performanceCountersTable.average_logger_processing_us += dur.Microseconds()
	performanceCountersTable.average_logger_processing_us >>= 1
}

func (perftable *PerformanceCountersTable) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("average_request_processing_us: %d\n", perftable.average_request_processing_us))
	sb.WriteString(fmt.Sprintf("average_response_processing_us: %d\n", perftable.average_response_processing_us))
	sb.WriteString(fmt.Sprintf("average_logger_processing_us: %d\n", perftable.average_logger_processing_us))
	sb.WriteString(fmt.Sprintf("requests_processed: %d\n", perftable.requests_processed))
	sb.WriteString(fmt.Sprintf("responses_processed: %d\n", perftable.responses_processed))
	sb.WriteString(fmt.Sprintf("loglines_processed: %d\n", perftable.loglines_processed))

	return sb.String()
}

var performanceCountersTable = newPerformanceCountersTable()

type RequestInspectionCallback func(*http.Request)
type ResponseInspectionCallback func(*http.Response)

var requestInspectionCallback = []RequestInspectionCallback{}
var responseInspectionCallback = []ResponseInspectionCallback{}

func RegisterRequestInspectionCallback(cb RequestInspectionCallback) {
	requestInspectionCallback = append(requestInspectionCallback, cb)
}

func RegisterResponseInspectionCallback(cb ResponseInspectionCallback) {
	responseInspectionCallback = append(responseInspectionCallback, cb)
}

func EnableWebHDFSTracking(events WebHDFSEventChannel) {
	RegisterRequestInspectionCallback(func(r *http.Request) { ProcessWebHDFSRequestQuery(r, events) })
}

func handleRequestCallbacks(req *http.Request) {
	for i := 0; i < len(requestInspectionCallback); i++ {
		requestInspectionCallback[i](req)
	}
}

func handleResponseCallbacks(res *http.Response) {
	for i := 0; i < len(responseInspectionCallback); i++ {
		responseInspectionCallback[i](res)
	}
}
