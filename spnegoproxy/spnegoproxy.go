package spnegoproxy

import (
	"bufio"
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	capi "github.com/hashicorp/consul/api"

	"github.com/matchaxnb/gokrb5/v8/client"
	"github.com/matchaxnb/gokrb5/v8/config"
	"github.com/matchaxnb/gokrb5/v8/keytab"
	"github.com/matchaxnb/gokrb5/v8/spnego"
)

// var logger = log.New(os.Stderr, "[spnegoproxy]", log.LstdFlags)

var logger *log.Logger
var DEBUGGING bool = false

const MAX_ERROR_COUNT = 20
const PAUSE_TIME_WHEN_ERROR = time.Minute * 1
const PAUSE_TIME_WHEN_NO_DATA = time.Millisecond * 300
const DT_MEMOIZE_TIME = 23 * time.Hour // prod: 23 * time.Hour and dev: 30 * time.Second

type SPNEGOClient struct {
	Client      *spnego.SPNEGO
	Destination string
	mu          sync.Mutex
}

type HadoopDT struct {
	Token struct {
		UrlString string `json:"urlString"`
	}
}
type HostPort struct {
	Host string
	Port int
}

type StringAndError struct {
	S string
	E error
}

// Memoizer caches the token for a given duration
type Memoizer[T any] struct {
	mu       sync.Mutex
	value    T
	err      error
	expireAt time.Time
	duration time.Duration
}

func NewMemoizer[T any](duration time.Duration) *Memoizer[T] {
	return &Memoizer[T]{duration: duration, expireAt: time.Now().Add(time.Hour * -1)}
}

func (m *Memoizer[T]) Get(fn func() (T, error)) (T, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// If cached value is still valid, return it
	if time.Now().After(m.expireAt) && m.err == nil {
		m.value, m.err = fn()
		m.expireAt = time.Now().Add(m.duration)
		logger.Println("Memoizer::Get -> recomputed values on", time.Now(), "will expire on", m.expireAt)
	}
	return m.value, m.err
}

func (e HostPort) f() string {
	return fmt.Sprintf("%s:%d", e.Host, e.Port)
}

func SetLogger(l *log.Logger) {
	logger = l
}
func _debugprintf(should bool, format string, a ...any) {
	if !should {
		return
	}
	logger.Printf(format, a...)
}

func Debugprintf(format string, a ...any) {
	_debugprintf(DEBUGGING, format, a...)
}

func BuildConsulClient(consulAddress *string, consulToken *string) *capi.Client {
	consulClient, err := capi.NewClient(&capi.Config{Address: *consulAddress, Scheme: "http", Token: *consulToken})
	if err != nil {
		logger.Panicf("Cannot connect to consul: %s", err)
	}
	return consulClient
}

// this builds a SPN client for the *last* valid host in the validHosts channel.
func BuildSPNClient(validHosts chan []HostPort, krbClient *client.Client, serviceType string) (spnClient *SPNEGOClient, realSpn string, realHost string, err error) {

	logger.Print("BuildSPNClient: Building a spn client")
	var spnHosts []HostPort

	// this eager loop loops until we have a valid element
	found := false
	for !found {
		select {
		case el := <-validHosts:
			spnHosts = el
		default:
			found = len(spnHosts) > 0
		}
	}
	logger.Printf("BuildSPNClient: SPN client built for known good host %s\n", spnHosts[0].f())
	spnStr := fmt.Sprintf("%s/%s", serviceType, spnHosts[0].Host)
	return &SPNEGOClient{
		Client: spnego.SPNEGOClient(krbClient, spnStr), Destination: spnHosts[0].f()}, spnStr, spnHosts[0].f(), nil
}

func LoadKrb5Config(keytabFile *string, cfgFile *string) (*keytab.Keytab, *config.Config) {
	keytab, err := keytab.Load(*keytabFile)
	if err != nil {
		logger.Printf("cannot read keytab: %s\n", err)
		logger.Panic("no keytab no dice")
	}
	conf, err := config.Load(*cfgFile)
	unsupErr := config.UnsupportedDirective{}
	if err != nil && !errors.As(err, &unsupErr) {
		logger.Printf("Bad config: %s\n", err)
		logger.Panic("no config no dice")
	}
	return keytab, conf
}

func (c *SPNEGOClient) GetAuthorizationToken() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.Client.AcquireCred(); err != nil {
		return "", fmt.Errorf("could not acquire client credential: %v", err)
	}
	token, err := c.Client.InitSecContext()
	if err != nil {
		return "", fmt.Errorf("could not initialize context: %v", err)
	}
	b, err := token.Marshal()
	if err != nil {
		return "", fmt.Errorf("could not marshal SPNEGO token: %v", err)
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func HostnameToChanHostPort(hostname string) chan []HostPort {
	messages := make(chan []HostPort)
	spl := strings.Split(hostname, ":")
	if len(spl) != 2 {
		logger.Panicf("Could not split %s by character : and get 2 bits", hostname)
	}
	portNum, err := strconv.Atoi(spl[1])
	if err != nil {
		logger.Panicf("Cannot parse %s to int: %s", spl[1], err)
	}
	hp := HostPort{spl[0], portNum}
	messages <- []HostPort{hp}
	return messages
}

func StartConsulGetService(client *capi.Client, serviceName string) chan []HostPort {
	messages := make(chan []HostPort, 8) // make that a buffered channel with maximum 8 messages in the backlog (in case we do something very wrong)
	serviceFunc := func(client *capi.Client, serviceName string, messages chan []HostPort) {
		for {
			Debugprintf("ConsulGetService: begin loop\n")
			healthyServices, meta, err := client.Health().Service(serviceName, "", true, &capi.QueryOptions{})
			if err != nil {
				logger.Printf("Cannot get healthy services for %#v (response meta: %#v) because of a consul error: %s", serviceName, meta, err)
				return
			}
			healthyStrings := make([]HostPort, len(healthyServices))
			for i := range healthyServices {
				Debugprintf("Adding healthy service: %#v\n", healthyServices[i].Node.Meta["fqdn"])
				healthyStrings[i] = HostPort{healthyServices[i].Node.Meta["fqdn"], healthyServices[i].Service.Port}
			}
			// flush existing messages because they're no longer relevant
			// we know there's a single writer to messages because we're it and we made it, so it's easy to flush
			if len(messages) > 0 {
				counter := 0
				for range messages {
					counter += 1
					Debugprintf("ConsulGetService: Flushing message #%d", counter)
				}
			}
			messages <- healthyStrings
			Debugprintf("ConsulGetService: added %d elements, now sleeping for 30 seconds\n", len(healthyStrings))
			time.Sleep(time.Second * 30)
		}
	}
	go serviceFunc(client, serviceName, messages)
	logger.Print("Started ConsulGetService")
	return messages
}

func enforceUserName(properUsername string, req *http.Request) {
	q := req.URL.Query()
	if q.Get("user.name") != properUsername {
		q.Set("user.name", properUsername)
		req.URL.RawQuery = q.Encode()
	}
	Debugprintf("[DEBUG] EnforceUserName, now request is %s\n", req.URL.RawQuery)
}

func EnforceUserName(properUsername string) {

	RegisterRequestInspectionCallback(func(r *http.Request) {
		enforceUserName(properUsername, r)
	})

}

func dropUsername(req *http.Request) {
	q := req.URL.Query()
	q.Del("user.name")
	req.URL.RawQuery = q.Encode()
	Debugprintf("[DEBUG] DropUsername, now request is %s", req.URL.RawQuery)
}

func DropUsername() {
	RegisterRequestInspectionCallback(dropUsername)
}

// theoretical implementation (slow AF)
/*
func demandDelegationTokenInResponse(res *http.Response) {
	rawLoc := res.Header.Get("Location")
	locationAddress, err := url.Parse(res.Header.Get("Location"))
	if err != nil || len(rawLoc) == 0 { // if you can't parse the Location header or there's none, I don't need to care
		return
	}
	qsValues := locationAddress.Query()
	if !qsValues.Has("delegation") {
		log.Panicf("Location redirection query string has no delegation token %s\n(original response follows)\n%v", locationAddress, res)
	} else {
		Debugprintf("Got delegation token in qsValues %v", qsValues)
	}
}*/

/*func demandDelegationTokenInResponse(res *http.Response) {
	defer updateResponseCallbacksPerformanceCounters(time.Now())
	loc := res.Header.Get("Location")
	if loc == "" {
		return
	}
	if !strings.Contains(loc, "&delegation=") || strings.Contains(loc, "&delegation=&") {
		log.Panicf("Location redirection query string has no delegation token %s\n(original response follows)\n%v", loc, res)
	}
}*/

func demandDelegationTokenInHeaders(headers *bytes.Reader) {
	defer updateResponseCallbacksPerformanceCounters(time.Now())

	buf, err := io.ReadAll(headers)
	if err != nil {
		return
	}
	headers.Seek(0, io.SeekStart)
	locIndex := bytes.Index(buf, []byte("\nLocation: "))
	delegationIndex := bytes.Index(buf, []byte("&delegation="))
	if locIndex == -1 {
		return
	}
	if delegationIndex == -1 || delegationIndex < locIndex {
		log.Panicf("Location redirection query string has no delegation token %s\n(original headers follows)\n%v", buf)
	}
}
func DemandDelegationTokenInResponse() {
	// RegisterResponseInspectionCallback(demandDelegationTokenInResponse)
	RegisterHeaderInspectionCallback(demandDelegationTokenInHeaders)
}

func DelegationTokenWorker(spnegoCli *SPNEGOClient, delegTokenChan chan<- StringAndError) {
	pleaseBreak := false
	dtMemoizer := NewMemoizer[string](DT_MEMOIZE_TIME)
	memoizeDelegationToken := func() (string, error) {
		getDt := func() (string, error) {
			delReq, err := http.NewRequest("GET", fmt.Sprintf("http://%s/webhdfs/v1/?op=GETDELEGATIONTOKEN", spnegoCli.Destination), nil)
			if err != nil {
				logger.Println(fmt.Errorf("DelegationTokenWorker: failed to build request: %v", err))
				panic(err)
			}

			client := http.Client{CheckRedirect: nil}
			token, err := spnegoCli.GetAuthorizationToken()
			if err != nil {
				logger.Println(fmt.Errorf("DelegationTokenWorker: failed to get authorization token: %v", err))
				panic(err)
			}

			authHeader := "Negotiate " + token

			delReq.Header.Set("Authorization", authHeader)
			delResp, err := client.Do(delReq)
			if err != nil {
				logger.Println(fmt.Errorf("DelegationTokenWorker: failed to get Delegation token: %v", err))
				return "", err
			}
			defer delResp.Body.Close()
			delBody, _ := io.ReadAll(delResp.Body)
			tokenObj := HadoopDT{}
			json.Unmarshal(delBody, &tokenObj)
			return tokenObj.Token.UrlString, nil
		}
		return dtMemoizer.Get(getDt)
	}
	for !pleaseBreak {
		r, err := memoizeDelegationToken()
		if err != nil {
			logger.Println(fmt.Errorf("DelegationTokenWorker got error: %v", err))
			pleaseBreak = true
		}

		delegTokenChan <- StringAndError{S: r, E: err}
	}
}

func HandleClient(conn *net.TCPConn, proxyHost string, spnegoCli *SPNEGOClient, delegationToken <-chan StringAndError, errCount *int) {

	if *errCount > MAX_ERROR_COUNT {
		log.Fatalf("Too many errors (%d), exiting", *errCount)
	}

	Debugprintf("new client: %v", conn.RemoteAddr())
	defer Debugprintf("stop processing request for client: %v", conn.RemoteAddr())

	defer conn.Close()
	proxyAddr, err := net.ResolveTCPAddr("tcp", proxyHost)
	if err != nil {
		logger.Printf("Cannot resolve proxy hostname %s -> %s", proxyHost, err)
		*errCount += 1
		return
	}

	proxyConn, err := net.DialTCP("tcp", nil, proxyAddr)
	if err != nil {
		logger.Printf("failed to connect to proxy: %v", err)
		*errCount += 1
		return
	}
	defer proxyConn.Close()
	reqReader := bufio.NewReader(conn)

	// get the SPNEGO token that we will use for this client

	if spnegoCli == nil {
		Debugprintf("no SPNEGO client is set, so no Kerberos auth happening (this is fine)")
	}

	processedCounter := 0
	var wg sync.WaitGroup
	pleaseBreak := false
	for !pleaseBreak {

		req, err := readRequestAndSetDelegation(reqReader, delegationToken)
		if err != nil && !errors.Is(err, io.EOF) {
			logger.Printf("failed to read request or to get SPNEGO token: %v", err)
			*errCount += 1
			time.Sleep(PAUSE_TIME_WHEN_NO_DATA)
			continue
		} else if err != nil && errors.Is(err, net.ErrClosed) {
			logger.Print("HandleClient: socket closed")
			break
		} else if errors.Is(err, io.EOF) {
			// just a simple break

			Debugprintf("EOF reached, breaking")
			break
		} else if err != nil {
			logger.Printf("Could not get request, will break: %v", err)
			break
		}

		Debugprintf("Read request: %s", req.URL)

		req.Host = proxyHost
		req.Header.Set("User-agent", "hadoop-proxy/0.1")
		req.Close = true
		handleRequestCallbacks(req) // needs to be synchronous
		req.WriteProxy(proxyConn)

		forward := func(from, to *net.TCPConn, tag string, isResponse bool) {
			defer wg.Done()
			startTime := time.Now()
			// defer to.CloseWrite()
			fromAddr, toAddr := from.RemoteAddr(), to.RemoteAddr()
			// handle request: a simple passthrough
			if !isResponse {
				defer updateRequestsPerformanceCounters(startTime)
				Debugprintf("[%s] request %s -> %s\n", tag, fromAddr, toAddr)
				io.Copy(to, from) // this is optimized but removes control
			} else { // handle response, slightly more complex
				defer updateResponsesPerformanceCounters(startTime)
				Debugprintf("[%s] response %s -> %s\n", tag, fromAddr, toAddr)
				// read the from
				resReader := bufio.NewReader(from)
				peekedBuf, err := resReader.Peek(1024)
				if err != io.EOF && err != nil {
					logger.Panicf("[%s] Could not peek response: %s", tag, err)
				}
				res, err := http.ReadResponse(resReader, nil)
				if err != nil {
					logger.Panicf("[%s] Could not read response: %s", tag, err)
				}
				if res.StatusCode > 400 && res.StatusCode != 404 {
					Debugprintf("Bad status code %d -> %v", res.StatusCode, res)
					*errCount += 1
					pleaseBreak = true // latch pleaseBreak, this is going to stop processing.
				} else {
					*errCount = 0
				}
				headerReader := bytes.NewReader(peekedBuf)
				handleResponseCallbacks(res)
				handleHeadersCallbacks(headerReader)
				res.Write(to)
			}
			logger.Printf("[%s] written\n", tag)
			to.CloseWrite()

		}
		wg.Add(2)
		go forward(conn, proxyConn, "local to proxied", false)
		go forward(proxyConn, conn, "proxied to local", true)
		processedCounter += 1
	}
	Debugprintf("Entering wg.Wait\n")
	wg.Wait()
	Debugprintf("[ProcessedCounter] Done waiting. Handled %d requests\n", processedCounter)
}

func readRequestAndSetDelegation(reqReader *bufio.Reader, delegationToken <-chan StringAndError) (*http.Request, error) {
	req, err := http.ReadRequest(reqReader)
	if err != nil {
		return nil, err
	}
	if strings.Contains(req.URL.RawQuery, "&delegation=") {
		return req, nil
	} else {
		sE := <-delegationToken
		if sE.E != nil {
			panic("Cannot get delegation token")
		}
		if len(sE.S) > 0 {
			req.URL.RawQuery += "&delegation=" + sE.S
		}
		return req, nil
	}
}
