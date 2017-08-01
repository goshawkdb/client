package client

import (
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/common/certs"
	"net"
	"time"
)

type conn struct {
	logger       log.Logger
	handshaker   *tlsCapnpHandshaker
	actor        common.ConnectionActor
	currentState connStateMachineComponent
	connDial
	connHandshake
	connRun
}

func newConnTCPTLSCapnpDialer(actor common.ConnectionActor, logger log.Logger, remoteHost string, clientCertAndKeyPEM, clusterCertPEM []byte) (*conn, error) {
	if _, _, err := net.SplitHostPort(remoteHost); err != nil {
		remoteHost = fmt.Sprintf("%v:%v", remoteHost, common.DefaultPort)
		_, _, err = net.SplitHostPort(remoteHost)
		if err != nil {
			return nil, err
		}
	}

	cert, privKey, err := certs.ExtractAndVerifyCertificate(clientCertAndKeyPEM)
	if err != nil {
		return nil, err
	}

	logger = log.With(logger, "subsystem", "connection", "protocol", "capnp", "remoteHost", remoteHost)
	phone := common.NewTCPDialer(nil, remoteHost, logger)
	yesman := newTLSCapnpHandshaker(phone, logger, actor, cert, privKey, clusterCertPEM)

	conn := &conn{
		logger:     logger,
		handshaker: yesman,
		actor:      actor,
	}

	conn.connDial.init(conn)
	conn.connHandshake.init(conn)
	conn.connRun.init(conn)

	conn.currentState = &conn.connDial

	return conn, nil
}

// state machine

type connStateMachineComponent interface {
	init(*conn)
	start() error
	connStateMachineComponentWitness()
}

func (conn *conn) nextState() {
	switch conn.currentState {
	case &conn.connDial:
		conn.currentState = &conn.connHandshake
	case &conn.connHandshake:
		conn.currentState = &conn.connRun
	default:
		panic(fmt.Sprintf("Unexpected current state for nextState: %v", conn.currentState))
	}
}

// Dial

type connDial struct {
	*conn
}

func (cd *connDial) connStateMachineComponentWitness() {}
func (cd *connDial) String() string                    { return "ConnDial" }

func (cd *connDial) init(conn *conn) {
	cd.conn = conn
}

func (cd *connDial) start() error {
	err := cd.handshaker.Dial()
	if err == nil {
		cd.nextState()
	}
	return err
}

// Handshake

type connHandshake struct {
	*conn
}

func (ch *connHandshake) connStateMachineComponentWitness() {}
func (ch *connHandshake) String() string                    { return "ConnHandshake" }

func (ch *connHandshake) init(conn *conn) {
	ch.conn = conn
}

func (ch *connHandshake) start() error {
	protocol, err := ch.handshaker.PerformHandshake()
	if err == nil {
		ch.protocol = protocol
		ch.nextState()
	}
	return err
}

// Run

type connRun struct {
	*conn
	protocol *tlsCapnpClient
}

func (cr *connRun) connStateMachineComponentWitness() {}
func (cr *connRun) String() string                    { return "ConnRun" }

func (cr *connRun) init(conn *conn) {
	cr.conn = conn
}

func (cr *connRun) start() error {
	return cr.protocol.Run()
}

// handshaker

func newTLSCapnpHandshaker(dialer *common.TCPDialer, logger log.Logger, actor common.ConnectionActor, cert *x509.Certificate, privKey *ecdsa.PrivateKey, clusterCertPEM []byte) *tlsCapnpHandshaker {
	return &tlsCapnpHandshaker{
		TLSCapnpHandshakerBase: common.NewTLSCapnpHandshakerBase(dialer),
		logger:                 logger,
		actor:                  actor,
		clientCert:             cert,
		clientPrivKey:          privKey,
		clusterCertPEM:         clusterCertPEM,
	}
}

type tlsCapnpHandshaker struct {
	*common.TLSCapnpHandshakerBase
	logger         log.Logger
	actor          common.ConnectionActor
	clientCert     *x509.Certificate
	clientPrivKey  *ecdsa.PrivateKey
	clusterCertPEM []byte
}

func (tch *tlsCapnpHandshaker) PerformHandshake() (*tlsCapnpClient, error) {
	helloSeg := tch.makeHello()
	if err := tch.Send(common.SegToBytes(helloSeg)); err != nil {
		return nil, err
	}

	if seg, err := tch.ReadExactlyOne(); err == nil {
		hello := msgs.ReadRootHello(seg)
		if tch.verifyHello(&hello) {
			if hello.IsClient() {
				return nil, errors.New("Remote peer is not a server!")

			} else {
				tcc := tch.newTLSCapnpClient()
				return tcc, tcc.finishHandshake()
			}

		} else {
			product := hello.Product()
			if l := len(common.ProductName); len(product) > l {
				product = product[:l] + "..."
			}
			version := hello.Version()
			if l := len(common.ProductVersion); len(version) > l {
				version = version[:l] + "..."
			}
			return nil, fmt.Errorf("Received erroneous hello from peer: received product name '%s' (expected '%s'), product version '%s' (expected '%s')",
				product, common.ProductName, version, common.ProductVersion)
		}
	} else {
		return nil, err
	}
}

func (tch *tlsCapnpHandshaker) makeHello() *capn.Segment {
	seg := capn.NewBuffer(nil)
	hello := msgs.NewRootHello(seg)
	hello.SetProduct(common.ProductName)
	hello.SetVersion(common.ProductVersion)
	hello.SetIsClient(true)
	return seg
}

func (tch *tlsCapnpHandshaker) verifyHello(hello *msgs.Hello) bool {
	return hello.Product() == common.ProductName &&
		hello.Version() == common.ProductVersion
}

func (tch *tlsCapnpHandshaker) newTLSCapnpClient() *tlsCapnpClient {
	return &tlsCapnpClient{
		tlsCapnpHandshaker: tch,
		logger:             tch.logger,
	}
}

// client

type tlsCapnpClient struct {
	*tlsCapnpHandshaker
	logger log.Logger
	reader *common.SocketReader
}

func (tcc *tlsCapnpClient) finishHandshake() error {
	roots := x509.NewCertPool()
	config := &tls.Config{
		Certificates: []tls.Certificate{
			tls.Certificate{
				Certificate: [][]byte{tcc.clientCert.Raw},
				PrivateKey:  tcc.clientPrivKey,
			},
		},
		CipherSuites:             []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
		RootCAs:                  roots,
		InsecureSkipVerify:       true,
	}

	if len(tcc.clusterCertPEM) != 0 && !roots.AppendCertsFromPEM(tcc.clusterCertPEM) {
		return errors.New("Unable to add cluster certificate to CA roots")
	}

	socket := tls.Client(tcc.Socket(), config)
	if err := socket.SetDeadline(time.Time{}); err != nil {
		return err
	}
	tcc.Dialer = common.NewTCPDialer(socket, tcc.Dialer.RemoteHost(), tcc.logger)

	if err := socket.Handshake(); err != nil {
		return err
	}

	if len(tcc.clusterCertPEM) != 0 {
		opts := x509.VerifyOptions{
			Roots:         roots,
			DNSName:       "", // disable server name checking
			Intermediates: x509.NewCertPool(),
		}
		certs := socket.ConnectionState().PeerCertificates
		for i, cert := range certs {
			if i == 0 {
				continue
			}
			opts.Intermediates.AddCert(cert)
		}
		if _, err := certs[0].Verify(opts); err != nil {
			return err
		}
	}

	return nil
}

func (tcc *tlsCapnpClient) ReadAndHandleOneMsg() error {
	seg, err := tcc.ReadOne()
	if err != nil {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return fmt.Errorf("Missed too many connection heartbeats. (%v)", netErr)
		} else {
			return err
		}
	}
	msg := msgs.ReadRootClientMessage(seg)
	switch which := msg.Which(); which {
	case msgs.CLIENTMESSAGE_HEARTBEAT:
		return nil // do nothing
	case msgs.CLIENTMESSAGE_CLIENTTXNOUTCOME:
		tcc.actor.EnqueueError(func() error {
			// todo something with msg.ClientTxnOutcome()
			panic("TODO")
			return nil
		})
		return nil
	default:
		return fmt.Errorf("Unexpected message type received from server: %v", which)
	}
}

func (tcc *tlsCapnpClient) Run() error {
	tcc.logger.Log("msg", "Connection established; awaiting roots.")

	seg := capn.NewBuffer(nil)
	message := msgs.NewRootClientMessage(seg)
	message.SetHeartbeat()
	tcc.CreateBeater(tcc.actor, common.SegToBytes(seg))
	tcc.createReader()
	return nil
}

func (tcc *tlsCapnpClient) createReader() {
	if tcc.reader == nil {
		tcc.reader = common.NewSocketReader(tcc.actor, tcc)
		tcc.reader.Start()
	}
}

func (tcc *tlsCapnpClient) InternalShutdown() {
	if tcc.reader != nil {
		tcc.reader.Stop()
		tcc.reader = nil
	}
	tcc.tlsCapnpHandshaker.InternalShutdown()
}
