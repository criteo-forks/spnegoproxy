package main

import (
	"flag"
	"log"
	"net"
	"os"

	"github.com/matchaxnb/gokrb5/v8/client"
	"github.com/matchaxnb/spnegoproxy/spnegoproxy"
)

var logger = log.New(os.Stderr, "", log.LstdFlags)

func main() {
	addr := flag.String("addr", "0.0.0.0:50070", "bind address")
	cfgFile := flag.String("config", "krb5.conf", "krb5 config file")
	user := flag.String("user", "your.user/your.host", "Kerberos user name")
	realm := flag.String("realm", "YOUR.REALM", "realm")
	toProxy := flag.String("proxy-service", "your-service-to-proxy", "host:port for the service to proxy to")
	spnServiceType := flag.String("spn-service-type", "HTTP", "SPN service type")
	keytabFile := flag.String("keytab-file", "krb5.keytab", "keytab file path")
	properUsername := flag.String("proper-username", "", "for WebHDFS, user.name value to force-set")
	dropUsername := flag.Bool("drop-username", false, "drop user.name from all queries")
	metricsAddrS := flag.String("metrics-addr", "", "optional address to expose a prometheus metrics endpoint")
	debug := flag.Bool("debug", true, "turn on debugging")
	flag.Parse()
	spnegoproxy.DEBUGGING = *debug
	keytab, conf := spnegoproxy.LoadKrb5Config(keytabFile, cfgFile)

	toProxyAsList := spnegoproxy.HostnameToChanHostPort(*toProxy)
	kclient := client.NewWithKeytab(*user, *realm, keytab, conf, client.Logger(logger), client.DisablePAFXFAST(false))
	kclient.Login()
	spnegoClient, spnEnabled, realHost, err := spnegoproxy.BuildSPNClient(toProxyAsList, kclient, *spnServiceType)
	if err != nil {
		logger.Panic("Cannot get SPN for service, failing")
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
	connListener, err := net.ListenTCP("tcp", listenAddr)
	if err != nil {
		logger.Panic(err)
	}
	eventChannel := make(spnegoproxy.WebHDFSEventChannel)
	if len(*metricsAddrS) > 0 {
		// we have a prometheus metrics endpoint
		logger.Print("Starting metrics handler")
		spnegoproxy.EnableWebHDFSTracking(eventChannel)
		spnegoproxy.ExposeMetrics(*metricsAddrS, eventChannel)
		go spnegoproxy.ConsumeWebHDFSEventStream(eventChannel)
	}

	if *dropUsername {
		spnegoproxy.DropUsername()
	} else if len(*properUsername) > 0 {
		spnegoproxy.EnforceUserName(*properUsername)
	}
	errorCount := 0
	defer connListener.Close()
	for {
		if errorCount > 1 {
			logger.Print("Renewing SPN client with new host because we had more than 1 error")
			for {
				logger.Print("Destroying old Kerberos client to force renewal")
				kclient.Destroy()
				logger.Print("Performing login again")
				kclient.Login()
				spnegoClient, _, realHost, err = spnegoproxy.BuildSPNClient(toProxyAsList, kclient, *spnServiceType)
				if err != nil {
					logger.Panic("Cannot get SPNEGOClient")
				}
				logger.Print("SPNEGOClient built successfully, moving on")
				break
			}
			errorCount = 0                  // reset the error counter for the time being
			logger.Printf("Now dealing with host %s for next connections\n", realHost)
		}

		conn, err := connListener.AcceptTCP()
		if err != nil {
			logger.Panic(err)
		}
		go spnegoproxy.HandleClient(conn, realHost, spnegoClient, &errorCount)
	}
}
