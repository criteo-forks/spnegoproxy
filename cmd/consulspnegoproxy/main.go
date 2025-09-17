package main

import (
	"errors"
	"flag"
	"log"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/matchaxnb/gokrb5/v8/client"
	"github.com/matchaxnb/spnegoproxy/spnegoproxy"
)

// var logger = log.New(os.Stderr, "", log.LstdFlags)
var logger *log.Logger

const ACCEPTABLE_CONSUL_ERRORS = 1
const MAXIMUM_OVERALL_ERRORS = 10

func main() {
	addr := flag.String("addr", "0.0.0.0:50070", "bind address")
	cfgFile := flag.String("config", "krb5.conf", "krb5 config file")
	user := flag.String("user", "your.user/your.host", "user name")
	realm := flag.String("realm", "YOUR.REALM", "realm")
	consulAddress := flag.String("consul-address", "your.consul.host:8500", "consul server address")
	consulToken := flag.String("consul-token", "", "consul access token (optional)")
	proxy := flag.String("proxy-service", "your-service-to-proxy", "proxy consul service")
	spnServiceType := flag.String("spn-service-type", "HTTP", "SPN service type")
	keytabFile := flag.String("keytab-file", "krb5.keytab", "keytab file path")
	properUsername := flag.String("proper-username", "", "for WebHDFS, user.name value to force-set")
	dropUsername := flag.Bool("drop-username", false, "drop user.name from all queries")
	metricsAddrS := flag.String("metrics-addr", "", "optional address to expose a prometheus metrics endpoint")
	demandDelegationToken := flag.Bool("demand-delegation-token", false, "demand delegation token in response Location headers")
	debug := flag.Bool("debug", false, "turn on debugging")
	disablePaxFast := flag.Bool("disable-pax-fast", false, "disable PAX fast, useful in some cases with Active Directory")
	flag.Parse()

	handler := slog.NewTextHandler(os.Stdout, nil)
	bufferedLogger := spnegoproxy.NewBufferedLogger(handler, 30*time.Second, 3000, 1024*1024*10)
	logger = spnegoproxy.NewStdLogger(bufferedLogger)

	spnegoproxy.SetLogger(logger)
	// start watchdog early
	watchdogChan := make(chan interface{})
	go func(c chan interface{}) {

		for {
			select {
			case <-c:
				// logger.Print("Watchdog-chan is happy")
			case <-time.After(10 * time.Second):
				logger.Print("Watchdog waited 10 seconds without getting news, panicking")
				logger.Panic("Watchdog timed out.")
			}
		}
	}(watchdogChan)

	// now proceed with the app startup
	spnegoproxy.DEBUGGING = *debug // enable or disable debugging
	keytab, conf := spnegoproxy.LoadKrb5Config(keytabFile, cfgFile)

	consulClient := spnegoproxy.BuildConsulClient(consulAddress, consulToken)
	realHosts := spnegoproxy.StartConsulGetService(consulClient, *proxy)
	kclient := client.NewWithKeytab(*user, *realm, keytab, conf, client.Logger(logger), client.DisablePAFXFAST(*disablePaxFast))
	kclient.Login()

	spnegoClient, spnEnabled, realHost, err := spnegoproxy.BuildSPNClient(realHosts, kclient, *spnServiceType)

	if err != nil {
		logger.Panic("Cannot get SPN for service in the first place, failing")
	}
	_, _, err = kclient.GetServiceTicket(spnEnabled)
	if err != nil {
		log.Panic("Cannot get service ticket, probably wrong config", err)
	}
	if *debug {
		logger.Printf("Listening on %s\n", *addr)
	}
	listenAddr, err := net.ResolveTCPAddr("tcp", *addr)
	if err != nil {
		logger.Panicf("Wrong TCP address %s -> %s", *addr, err)
	}
	eventChannel := make(spnegoproxy.WebHDFSEventChannel)
	if len(*metricsAddrS) > 0 {
		// we have a prometheus metrics endpoint
		spnegoproxy.Debugprintf("Starting metrics handler")
		spnegoproxy.EnableWebHDFSTracking(eventChannel)
		spnegoproxy.ExposeMetrics(*metricsAddrS, eventChannel)
		go spnegoproxy.ConsumeWebHDFSEventStream(eventChannel)
	}

	connListener, err := net.ListenTCP("tcp", listenAddr)

	deadlineDuration := 3 * time.Second
	if err != nil {
		logger.Panic(err)
	}

	if *dropUsername {
		spnegoproxy.DropUsername()
	} else if len(*properUsername) > 0 {
		spnegoproxy.EnforceUserName(*properUsername)
	}

	errorCount := 0
	overallErrorCount := 0
	preFail := 0
	skipped := 0

	defer connListener.Close()
	for {
		watchdogChan <- nil // ensure watchdog-chan is happy
		if skipped >= 10 {
			logger.Print("10 TCP connections skipped, exiting to renew")
			os.Exit(2)
		}
		if errorCount > 0 {
			logger.Print("Renewing SPN client with new host because we had an error")
			for {
				spnegoproxy.Debugprintf("Destroying old Kerberos client to force renewal\n")
				kclient.Destroy()
				spnegoproxy.Debugprintf("Performing login again\n")
				kclient.Login()
				logger.Printf("New credentials for %s valid until: %s", kclient.Credentials.UserName(), kclient.Credentials.ValidUntil())
				spnegoClient, _, realHost, err = spnegoproxy.BuildSPNClient(realHosts, kclient, *spnServiceType)
				if err != nil && preFail <= ACCEPTABLE_CONSUL_ERRORS {
					logger.Println("Cannot get SPN client for service after error, sleeping before we retry")
					time.Sleep(30 * time.Second)
					preFail += 1
				} else if err != nil && preFail >= ACCEPTABLE_CONSUL_ERRORS {
					logger.Fatalf("cannot get valid hostname for consul service after %d tries, exiting.\n", ACCEPTABLE_CONSUL_ERRORS)
				}
				if err == nil {
					logger.Printf("SPNEGOClient built successfully after %d errors, moving on\n", preFail)
					preFail = 0 // now we're in the happy case
					break
				}
			}
			overallErrorCount += errorCount // transfer the error count to that overall counter
			errorCount = 0                  // reset the error counter for the time being
			spnegoproxy.Debugprintf("Now dealing with host %s for next connections\n", realHost)
		}
		connListener.SetDeadline(time.Now().Add(deadlineDuration))
		conn, err := connListener.AcceptTCP()
		if err != nil && errors.Is(err, os.ErrDeadlineExceeded) {
			spnegoproxy.Debugprintf("No new TCP connection, skipping.")
			skipped += 1
			spnegoproxy.Debugprintf("skipped: %d\n", skipped)
			continue
		} else if err != nil {
			logger.Panic(err)
		} else {
			skipped = 0
		}

		if *demandDelegationToken {
			spnegoproxy.DemandDelegationTokenInResponse()
		}
		go spnegoproxy.HandleClient(conn, realHost, spnegoClient, &errorCount)
		if MAXIMUM_OVERALL_ERRORS <= overallErrorCount {
			logger.Fatalf("Reached error count %d > %d, exiting.\n", overallErrorCount, MAXIMUM_OVERALL_ERRORS)
		}
	}
}
