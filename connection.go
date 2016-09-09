package client

import (
	"bytes"
	"crypto/ecdsa"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	cc "github.com/msackman/chancell"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
	"goshawkdb.io/common/certs"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

// Connection represents a connection to the GoshawkDB server. A
// connection may only be used by one go-routine at a time.
type Connection struct {
	lock              sync.RWMutex
	curTxn            *Txn
	nextVUUId         uint64
	nextTxnId         uint64
	namespace         []byte
	rootVUUIds        map[string]*refCap
	socket            net.Conn
	cache             *cache
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(connectionMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan connectionMsg
	rng               *rand.Rand
	clientCert        *x509.Certificate
	clientPrivKey     *ecdsa.PrivateKey
	clusterCertPEM    []byte
	password          []byte
	currentState      connectionStateMachineComponent
	connectionAwaitHandshake
	connectionAwaitServerHandshake
	connectionRun
}

// Create a new connection. The hostPort parameter can be either
// hostname:port or ip:port. If port is not provided the default port
// of 7894 is used. This will block until a connection is established
// and ready to use, or an error occurs. The clientCertAndKeyPEM
// parameter should be the client certificate followed by private key
// in PEM format. The clusterCert parameter is the cluster
// certificate. This is optional, but recommended: without it, the
// client will not be able to verify the server to which it connects.
func NewConnection(hostPort string, clientCertAndKeyPEM, clusterCertPEM []byte) (*Connection, error) {
	if _, _, err := net.SplitHostPort(hostPort); err != nil {
		hostPort = fmt.Sprintf("%v:%v", hostPort, common.DefaultPort)
		_, _, err = net.SplitHostPort(hostPort)
		if err != nil {
			return nil, err
		}
	}
	tcpAddr, err := net.ResolveTCPAddr("tcp", hostPort)
	if err != nil {
		return nil, err
	}

	cert, privKey, err := certs.ExtractAndVerifyCertificate(clientCertAndKeyPEM)
	if err != nil {
		return nil, err
	}

	socket, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	if err = common.ConfigureSocket(socket); err != nil {
		return nil, err
	}

	conn := &Connection{
		nextVUUId:      0,
		nextTxnId:      0,
		socket:         socket,
		cache:          newCache(),
		rng:            rand.New(rand.NewSource(time.Now().UnixNano())),
		clientCert:     cert,
		clientPrivKey:  privKey,
		clusterCertPEM: clusterCertPEM,
	}
	var head *cc.ChanCellHead
	head, conn.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan connectionMsg, n)
			cell.Open = func() { conn.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			conn.enqueueQueryInner = func(msg connectionMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
				if curCell == cell {
					select {
					case queryChan <- msg:
						return true, nil
					default:
						return false, nil
					}
				} else {
					return false, cont
				}
			}
		})

	go conn.actorLoop(head)
	if err = conn.awaitReady(); err != nil {
		return nil, err
	}
	return conn, nil
}

// Run a transaction. The transaction is the function supplied, and
// the function is invoked potentially several times until it
// completes successfully: either committing or choosing to abort. The
// function should therefore be referentially transparent. Returning
// any non-nil error will cause the transaction to be aborted with the
// only exception that returning Restart when the transaction has
// identified a restart is required will cause the transaction to be
// immediately restarted.
//
// The function final results are returned by this function, along
// with statistics regarding how the transaction proceeded.
//
// This function automatically detects and creates nested
// transactions: it is perfectly safe and expected to call
// RunTransaction from within a transaction.
func (conn *Connection) RunTransaction(fun func(*Txn) (interface{}, error)) (interface{}, *Stats, error) {
	roots := conn.rootVarUUIds()
	if roots == nil {
		return nil, nil, fmt.Errorf("Unable to start transaction: root objects not ready")
	}
	var oldTxn *Txn
	conn.lock.Lock()
	txn := newTxn(fun, conn, conn.cache, roots, conn.curTxn)
	conn.curTxn, oldTxn = txn, conn.curTxn
	conn.lock.Unlock()
	res, stats, err := txn.run()
	conn.lock.Lock()
	conn.curTxn = oldTxn
	conn.lock.Unlock()
	return res, stats, err
}

func (conn *Connection) rootVarUUIds() map[string]*refCap {
	conn.lock.RLock()
	defer conn.lock.RUnlock()
	return conn.rootVUUIds
}

func (conn *Connection) nextVarUUId() *common.VarUUId {
	conn.lock.Lock()
	defer conn.lock.Unlock()
	binary.BigEndian.PutUint64(conn.namespace[:8], conn.nextVUUId)
	vUUId := common.MakeVarUUId(conn.namespace)
	conn.nextVUUId++
	return vUUId
}

type connectionMsg interface {
	witness() connectionMsg
}

type connectionMsgBasic struct{}

func (cmb connectionMsgBasic) witness() connectionMsg { return cmb }

type connectionMsgShutdown struct{ connectionMsgBasic }

func (cms connectionMsgShutdown) Error() string {
	return "Connection Shutdown"
}

// Shutdown the connection. This will block until the connection is
// closed.
func (conn *Connection) Shutdown() {
	if conn.enqueueQuery(connectionMsgShutdown{}) {
		conn.cellTail.Wait()
	}
}

type connectionMsgSyncQuery struct {
	connectionMsgBasic
	resultChan chan struct{}
	err        error
}

func (cmsq *connectionMsgSyncQuery) init() {
	cmsq.resultChan = make(chan struct{})
}

type connectionMsgAwaitReady struct{ connectionMsgSyncQuery }

func (conn *Connection) awaitReady() error {
	query := &connectionMsgAwaitReady{}
	query.init()
	if conn.enqueueSyncQuery(query, query.resultChan) {
		return query.err
	} else {
		return nil
	}
}

type connectionMsgTxn struct {
	connectionMsgSyncQuery
	txn          *msgs.ClientTxn
	outcome      *msgs.ClientTxnOutcome
	modifiedVars []*common.VarUUId
}

func (cmt *connectionMsgTxn) setOutcomeError(outcome *msgs.ClientTxnOutcome, modifiedVars []*common.VarUUId, err error) bool {
	select {
	case <-cmt.resultChan:
		return false
	default:
		cmt.outcome = outcome
		cmt.modifiedVars = modifiedVars
		cmt.err = err
		close(cmt.resultChan)
		return true
	}
}

func (conn *Connection) submitTransaction(txn *msgs.ClientTxn) (*msgs.ClientTxnOutcome, []*common.VarUUId, error) {
	query := &connectionMsgTxn{txn: txn}
	query.init()
	if conn.enqueueSyncQuery(query, query.resultChan) {
		return query.outcome, query.modifiedVars, query.err
	} else {
		return nil, nil, connectionMsgShutdown{}
	}
}

func (conn *Connection) enqueueQuery(msg connectionMsg) bool {
	var f cc.CurCellConsumer
	f = func(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
		return conn.enqueueQueryInner(msg, cell, f)
	}
	return conn.cellTail.WithCell(f)
}

func (conn *Connection) enqueueSyncQuery(msg connectionMsg, resultChan chan struct{}) bool {
	if conn.enqueueQuery(msg) {
		select {
		case <-resultChan:
			return true
		case <-conn.cellTail.Terminated:
			return false
		}
	} else {
		return false
	}
}

// actor loop

func (conn *Connection) actorLoop(head *cc.ChanCellHead) {
	conn.init()
	var (
		err          error
		stateChanged bool
		oldState     connectionStateMachineComponent
		queryChan    <-chan connectionMsg
		queryCell    *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = conn.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	for !terminate {
		stateChanged = oldState != conn.currentState
		if stateChanged {
			oldState = conn.currentState
			terminate, err = conn.currentState.start()
		} else {
			if msg, ok := <-queryChan; ok {
				terminate, err = conn.handleMsg(msg)
			} else {
				head.Next(queryCell, chanFun)
			}
		}
		terminate = terminate || err != nil
	}
	conn.cellTail.Terminate()
	conn.handleShutdown(err)
}

func (conn *Connection) handleMsg(msg connectionMsg) (terminate bool, err error) {
	switch msgT := msg.(type) {
	case *connectionMsgTxn:
		err = conn.submitTxn(msgT)
	case connectionReadMessage:
		err = conn.handleMsgFromServer((msgs.ClientMessage)(msgT))
	case connectionReadError:
		conn.reader = nil
		err = msgT.error
	case *connectionBeater:
		err = conn.beat()
	case connectionMsgShutdown:
		terminate = true
		conn.currentState = nil
	case *connectionMsgAwaitReady:
		conn.isReady(msgT)
	default:
		err = fmt.Errorf("Received unexpected message: %v", msgT)
	}
	return
}

func (conn *Connection) handleShutdown(err error) {
	if err != nil {
		log.Println("Connection error:", err)
	}
	conn.maybeStopBeater()
	conn.maybeStopReaderAndCloseSocket()
	e := fmt.Errorf("Connection shutting down")
	if conn.liveTxn != nil {
		if !conn.liveTxn.setOutcomeError(nil, nil, e) {
			panic("Internal Logic Failure when closing outstanding live txn")
		}
		conn.liveTxn = nil
	}
	if conn.awaiting != nil {
		conn.awaiting.err = e
		close(conn.awaiting.resultChan)
		conn.awaiting = nil
	}
}

// connection state machine

type connectionStateMachineComponent interface {
	init(*Connection)
	start() (bool, error)
	connectionStateMachineComponentWitness()
}

func (conn *Connection) init() {
	conn.connectionAwaitHandshake.init(conn)
	conn.connectionAwaitServerHandshake.init(conn)
	conn.connectionRun.init(conn)

	conn.currentState = &conn.connectionAwaitHandshake
}

func (conn *Connection) nextState() {
	switch conn.currentState {
	case &conn.connectionAwaitHandshake:
		conn.currentState = &conn.connectionAwaitServerHandshake
	case &conn.connectionAwaitServerHandshake:
		conn.currentState = &conn.connectionRun
	}
}

// connectionAwaitHandshake

type connectionAwaitHandshake struct {
	*Connection
}

func (cah *connectionAwaitHandshake) connectionStateMachineComponentWitness() {}
func (cah *connectionAwaitHandshake) String() string                          { return "ConnectionAwaitHandshake" }

func (cah *connectionAwaitHandshake) init(conn *Connection) {
	cah.Connection = conn
}

func (cah *connectionAwaitHandshake) start() (bool, error) {
	helloSeg, err := cah.makeHello()
	if err != nil {
		return false, err
	}
	buf := new(bytes.Buffer)
	if _, err := helloSeg.WriteTo(buf); err != nil {
		return false, err
	}
	if err := cah.send(buf.Bytes()); err != nil {
		return false, err
	}

	if seg, err := capn.ReadFromStream(cah.socket, nil); err == nil {
		if hello := msgs.ReadRootHello(seg); cah.verifyHello(&hello) {
			cah.nextState()
		} else {
			product := hello.Product()
			if l := len(common.ProductName); len(product) > l {
				product = product[:l] + "..."
			}
			version := hello.Version()
			if l := len(common.ProductVersion); len(version) > l {
				version = version[:l] + "..."
			}
			return false, fmt.Errorf("Received erroneous hello from peer: received product name '%s' (expected '%s'), product version '%s' (expected '%s')",
				product, common.ProductName, version, common.ProductVersion)
		}
	} else {
		return false, err
	}

	return false, nil
}

func (cah *connectionAwaitHandshake) makeHello() (*capn.Segment, error) {
	seg := capn.NewBuffer(nil)
	hello := msgs.NewRootHello(seg)
	hello.SetProduct(common.ProductName)
	hello.SetVersion(common.ProductVersion)
	hello.SetIsClient(true)
	return seg, nil
}

func (cah *connectionAwaitHandshake) verifyHello(hello *msgs.Hello) bool {
	return hello.Product() == common.ProductName &&
		hello.Version() == common.ProductVersion &&
		!hello.IsClient()
}

func (cah *connectionAwaitHandshake) send(msg []byte) error {
	l := len(msg)
	for l > 0 {
		switch w, err := cah.socket.Write(msg); {
		case err != nil:
			return err
		case w == l:
			return nil
		default:
			msg = msg[w:]
			l -= w
		}
	}
	return nil
}

func (cah *connectionAwaitHandshake) readOne() (*capn.Segment, error) {
	return capn.ReadFromStream(cah.socket, nil)
}

// Await Server Handshake

type connectionAwaitServerHandshake struct {
	*Connection
}

func (cash *connectionAwaitServerHandshake) connectionStateMachineComponentWitness() {}
func (cash *connectionAwaitServerHandshake) String() string                          { return "ConnectionAwaitServerHandshake" }

func (cash *connectionAwaitServerHandshake) init(conn *Connection) {
	cash.Connection = conn
}

func (cash *connectionAwaitServerHandshake) start() (bool, error) {
	roots := x509.NewCertPool()
	config := &tls.Config{
		Certificates: []tls.Certificate{
			tls.Certificate{
				Certificate: [][]byte{cash.clientCert.Raw},
				PrivateKey:  cash.clientPrivKey,
			},
		},
		CipherSuites:             []uint16{tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256},
		MinVersion:               tls.VersionTLS12,
		PreferServerCipherSuites: true,
		RootCAs:                  roots,
		InsecureSkipVerify:       true,
	}

	if len(cash.clusterCertPEM) != 0 && !roots.AppendCertsFromPEM(cash.clusterCertPEM) {
		return false, fmt.Errorf("Unable to add cluster certificate to CA roots")
	}

	socket := tls.Client(cash.socket, config)
	if err := socket.SetDeadline(time.Time{}); err != nil {
		return false, err
	}
	cash.socket = socket

	if err := socket.Handshake(); err != nil {
		return false, err
	}

	if len(cash.clusterCertPEM) != 0 {
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
			return false, err
		}
	}

	if seg, err := cash.readOne(); err == nil {
		server := msgs.ReadRootHelloClientFromServer(seg)
		rootsCap := server.Roots()
		l := rootsCap.Len()
		if l == 0 {
			return false, fmt.Errorf("Cluster is not yet formed; Root objects have not been created.")
		}
		roots := make(map[string]*refCap, l)
		for idx := 0; idx < l; idx++ {
			rootCap := rootsCap.At(idx)
			roots[rootCap.Name()] = &refCap{
				vUUId:      common.MakeVarUUId(rootCap.VarId()),
				capability: common.NewCapability(rootCap.Capability()),
			}
		}
		cash.lock.Lock()
		cash.rootVUUIds = roots
		cash.namespace = make([]byte, common.KeyLen)
		copy(cash.namespace[8:], server.Namespace())
		cash.lock.Unlock()
		cash.nextState()
		return false, nil

	} else {
		return false, err
	}
}

// connectionRun

type connectionRun struct {
	*Connection
	beater       *connectionBeater
	reader       *connectionReader
	mustSendBeat bool
	missingBeats int
	beatBytes    []byte
	awaiting     *connectionMsgAwaitReady
	liveTxn      *connectionMsgTxn
}

func (cr *connectionRun) connectionStateMachineComponentWitness() {}
func (cr *connectionRun) String() string                          { return "ConnectionRun" }

func (cr *connectionRun) init(conn *Connection) {
	cr.Connection = conn
}

func (cr *connectionRun) start() (bool, error) {
	log.Printf("Connection established to %v\n", cr.socket.RemoteAddr())

	seg := capn.NewBuffer(nil)
	message := msgs.NewRootClientMessage(seg)
	message.SetHeartbeat()
	buf := new(bytes.Buffer)
	_, err := seg.WriteTo(buf)
	if err != nil {
		return false, err
	}
	cr.beatBytes = buf.Bytes()

	cr.mustSendBeat = true
	cr.missingBeats = 0

	cr.beater = newConnectionBeater(cr.Connection)
	go cr.beater.beat()

	cr.reader = newConnectionReader(cr.Connection)
	go cr.reader.read()

	if cr.awaiting != nil {
		close(cr.awaiting.resultChan)
		cr.awaiting = nil
	}

	return false, nil
}

func (cr *connectionRun) isReady(ar *connectionMsgAwaitReady) {
	if cr.currentState == cr {
		close(ar.resultChan)
	} else {
		cr.awaiting = ar
	}
}

func (cr *connectionRun) sendMessage(msg *msgs.ClientMessage) error {
	if cr.currentState == cr {
		cr.mustSendBeat = false
		buf := new(bytes.Buffer)
		if _, err := msg.Segment.WriteTo(buf); err != nil {
			return err
		}
		return cr.send(buf.Bytes())
	}
	return nil
}

func (cr *connectionRun) handleMsgFromServer(msg msgs.ClientMessage) error {
	if cr.currentState != cr {
		return nil
	}
	cr.missingBeats = 0
	switch which := msg.Which(); which {
	case msgs.CLIENTMESSAGE_HEARTBEAT:
		// do nothing
	case msgs.CLIENTMESSAGE_CLIENTTXNOUTCOME:
		return cr.handleTxnOutcome(msg.ClientTxnOutcome())
	default:
		return fmt.Errorf("Received unexpected message from server: %v", which)
	}
	return nil
}

func (cr *connectionRun) submitTxn(txnMsg *connectionMsgTxn) error {
	if cr.currentState != cr {
		if !txnMsg.setOutcomeError(nil, nil, fmt.Errorf("Connection in wrong state: %v", cr.currentState)) {
			return fmt.Errorf("Connection in wrong state: %v", cr.currentState)
		}
		return nil
	}
	if cr.liveTxn != nil {
		if !txnMsg.setOutcomeError(nil, nil, fmt.Errorf("Existing live txn")) {
			return fmt.Errorf("Existing live txn")
		}
		return nil
	}
	binary.BigEndian.PutUint64(cr.namespace[:8], cr.nextTxnId)
	txnMsg.txn.SetId(cr.namespace)
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootClientMessage(seg)
	msg.SetClientTxnSubmission(*txnMsg.txn)
	if err := cr.sendMessage(&msg); err == nil {
		cr.liveTxn = txnMsg
		return nil
	} else {
		cr.nextTxnId++
		return err
	}
}

func (cr *connectionRun) handleTxnOutcome(outcome msgs.ClientTxnOutcome) error {
	txnId := common.MakeTxnId(outcome.Id())
	if cr.liveTxn == nil {
		return fmt.Errorf("Received txn outcome for unknown txn: %v", txnId)
	}
	finalTxnId := common.MakeTxnId(outcome.FinalId())
	if !bytes.Equal(cr.liveTxn.txn.Id(), outcome.Id()) {
		return fmt.Errorf("Received txn outcome for wrong txn: %v (expecting %v) (final %v) (which %v)", txnId, common.MakeTxnId(cr.liveTxn.txn.Id()), finalTxnId, outcome.Which())
	}
	var err error
	final := binary.BigEndian.Uint64(finalTxnId[:8])
	if final < cr.nextTxnId {
		return fmt.Errorf("Final (%v) < next (%v)\n", final, cr.nextTxnId)
	}
	cr.nextTxnId = final + 1 + uint64(cr.rng.Intn(8))
	var modifiedVars []*common.VarUUId
	switch outcome.Which() {
	case msgs.CLIENTTXNOUTCOME_COMMIT:
		cr.cache.updateFromTxnCommit(cr.liveTxn.txn, finalTxnId)
	case msgs.CLIENTTXNOUTCOME_ABORT:
		updates := outcome.Abort()
		modifiedVars = cr.cache.updateFromTxnAbort(&updates)
	case msgs.CLIENTTXNOUTCOME_ERROR:
		err = fmt.Errorf(outcome.Error())
	}
	if !cr.liveTxn.setOutcomeError(&outcome, modifiedVars, err) {
		return fmt.Errorf("Live txn already closed")
	}
	cr.liveTxn = nil
	return nil
}

func (cr *connectionRun) beat() error {
	if cr.currentState != cr {
		return nil
	}
	if cr.missingBeats == 2 {
		return fmt.Errorf("Missed too many connection heartbeats.")
	}
	cr.missingBeats++
	if cr.mustSendBeat {
		return cr.send(cr.beatBytes)
	} else {
		cr.mustSendBeat = true
	}
	return nil
}

func (cr *connectionRun) maybeStopBeater() {
	if cr.beater != nil {
		close(cr.beater.terminate)
		cr.beater.terminated.Wait()
		cr.beater = nil
	}
}

func (cr *connectionRun) maybeStopReaderAndCloseSocket() {
	if cr.reader != nil {
		close(cr.reader.terminate)
		cr.reader.terminated.Wait()
		cr.reader = nil
	}
	if cr.socket != nil {
		cr.socket.Close()
		cr.socket = nil
	}
}

// Beater

type connectionBeater struct {
	*Connection
	terminate  chan struct{}
	terminated *sync.WaitGroup
	ticker     *time.Ticker
}

func newConnectionBeater(conn *Connection) *connectionBeater {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	return &connectionBeater{
		Connection: conn,
		terminate:  make(chan struct{}),
		terminated: wg,
		ticker:     time.NewTicker(common.HeartbeatInterval),
	}
}

func (cb *connectionBeater) beat() {
	defer func() {
		cb.ticker.Stop()
		cb.ticker = nil
		cb.terminated.Done()
	}()
	for {
		select {
		case <-cb.terminate:
			return
		case <-cb.ticker.C:
			if !cb.enqueueQuery(cb) {
				return
			}
		}
	}
}

func (cb *connectionBeater) witness() connectionMsg { return cb }

// Reader

type connectionReader struct {
	*Connection
	terminate  chan struct{}
	terminated *sync.WaitGroup
}

func newConnectionReader(conn *Connection) *connectionReader {
	wg := new(sync.WaitGroup)
	wg.Add(1)
	return &connectionReader{
		Connection: conn,
		terminate:  make(chan struct{}),
		terminated: wg,
	}
}

func (cr *connectionReader) read() {
	defer cr.terminated.Done()
	for {
		select {
		case <-cr.terminate:
			return
		default:
			if seg, err := cr.readOne(); err == nil {
				msg := msgs.ReadRootClientMessage(seg)
				if !cr.enqueueQuery(connectionReadMessage(msg)) {
					return
				}
			} else {
				cr.enqueueQuery(connectionReadError{err})
				return
			}
		}
	}
}

type connectionReadMessage msgs.ClientMessage

func (crm connectionReadMessage) witness() connectionMsg { return crm }

type connectionReadError struct {
	error
}

func (cre connectionReadError) witness() connectionMsg { return cre }
