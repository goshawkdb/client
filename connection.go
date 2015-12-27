package client

import (
	"bytes"
	cr "crypto/rand"
	"encoding/binary"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	cc "github.com/msackman/chancell"
	"golang.org/x/crypto/nacl/box"
	"golang.org/x/crypto/nacl/secretbox"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
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
	serverHost        string
	rmId              common.RMId
	namespace         []byte
	rootVUUId         *common.VarUUId
	socket            *net.TCPConn
	cache             *cache
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(connectionMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan connectionMsg
	rng               *rand.Rand
	username          string
	password          []byte
	currentState      connectionStateMachineComponent
	connectionAwaitHandshake
	connectionAwaitServerHandshake
	connectionRun
}

// Create a new connection. The hostPort parameter can be either
// hostname:port or ip:port. If port is not provided the default port
// of 7894 is used. This will block until a connection is established
// and ready to use, or an error occurs.
func NewConnection(hostPort, username string, password []byte) (*Connection, error) {
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
	socket, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		return nil, err
	}
	if err = socket.SetKeepAlive(true); err != nil {
		return nil, err
	}
	if err = socket.SetKeepAlivePeriod(time.Second); err != nil {
		return nil, err
	}

	conn := &Connection{
		nextVUUId: 0,
		nextTxnId: 0,
		socket:    socket,
		cache:     newCache(),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
		username:  username,
		password:  password,
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
	root := conn.rootVarUUId()
	if root == nil {
		return nil, nil, fmt.Errorf("Unable to start transaction: root object not ready")
	}
	var oldTxn *Txn
	conn.lock.Lock()
	txn := newTxn(fun, conn, conn.cache, root, conn.curTxn)
	conn.curTxn, oldTxn = txn, conn.curTxn
	conn.lock.Unlock()
	res, stats, err := txn.run()
	conn.lock.Lock()
	conn.curTxn = oldTxn
	conn.lock.Unlock()
	return res, stats, err
}

func (conn *Connection) rootVarUUId() *common.VarUUId {
	conn.lock.RLock()
	defer conn.lock.RUnlock()
	return conn.rootVUUId
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
	connectionMsgWitness()
}

type connectionMsgShutdown struct{}

func (cms *connectionMsgShutdown) connectionMsgWitness() {}
func (cms *connectionMsgShutdown) Error() string {
	return "Connection Shutdown"
}

var connectionMsgShutdownInst = &connectionMsgShutdown{}

// Shutdown the connection. This will block until the connection is
// closed.
func (conn *Connection) Shutdown() {
	if conn.enqueueQuery(connectionMsgShutdownInst) {
		conn.cellTail.Wait()
	}
}

type connectionMsgSyncQuery struct {
	resultChan chan struct{}
	err        error
}

func (cmsq *connectionMsgSyncQuery) init() {
	cmsq.resultChan = make(chan struct{})
}

type connectionMsgAwaitReady connectionMsgSyncQuery

func (cmwr *connectionMsgAwaitReady) connectionMsgWitness() {}

func (conn *Connection) awaitReady() error {
	query := new(connectionMsgSyncQuery)
	query.init()
	if conn.enqueueSyncQuery((*connectionMsgAwaitReady)(query), query.resultChan) {
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

func (cmt *connectionMsgTxn) connectionMsgWitness() {}

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
		return nil, nil, connectionMsgShutdownInst
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
	case *connectionReadError:
		conn.reader = nil
		err = msgT.error
	case *connectionReadMessage:
		err = conn.handleMsgFromServer((*msgs.ClientMessage)(msgT))
	case *connectionBeater:
		err = conn.beat()
	case *connectionMsgShutdown:
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
			log.Println("Internal Logic Failure when closing outstanding live txn")
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
	case &conn.connectionRun:
		conn.currentState = nil
	}
}

// connectionAwaitHandshake

type connectionAwaitHandshake struct {
	*Connection
	privateKey  *[32]byte
	sessionKey  *[32]byte
	nonce       uint64
	nonceAryIn  *[24]byte
	nonceAryOut *[24]byte
	outBuff     []byte
	inBuff      []byte
}

func (cah *connectionAwaitHandshake) connectionStateMachineComponentWitness() {}

func (cah *connectionAwaitHandshake) init(conn *Connection) {
	cah.Connection = conn
	nonceAryIn := [24]byte{}
	cah.nonceAryIn = &nonceAryIn
	nonceAryOut := [24]byte{}
	cah.nonceAryOut = &nonceAryOut
	cah.inBuff = make([]byte, 16)
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
			sessionKey := [32]byte{}
			remotePublicKey := [32]byte{}
			copy(remotePublicKey[:], hello.PublicKey())
			box.Precompute(&sessionKey, &remotePublicKey, cah.privateKey)
			cah.sessionKey = &sessionKey
			cah.nonceAryOut[0] = 128
			cah.nonce = 0
			cah.nextState()
		} else {
			return false, fmt.Errorf("Received erroneous hello from server")
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
	publicKey, privateKey, err := box.GenerateKey(cr.Reader)
	if err != nil {
		return nil, err
	}
	cah.privateKey = privateKey
	hello.SetPublicKey(publicKey[:])
	hello.SetIsClient(true)
	return seg, nil
}

func (cah *connectionAwaitHandshake) verifyHello(hello *msgs.Hello) bool {
	return hello.Product() == common.ProductName &&
		hello.Version() == common.ProductVersion &&
		!hello.IsClient()
}

func (cah *connectionAwaitHandshake) send(msg []byte) (err error) {
	if cah.sessionKey == nil {
		_, err = cah.socket.Write(msg)
	} else {
		reqLen := len(msg) + secretbox.Overhead + 16
		if cah.outBuff == nil {
			cah.outBuff = make([]byte, 16, reqLen)
		} else if l := len(cah.outBuff); l < reqLen {
			cah.outBuff = make([]byte, 16, l*(1+(reqLen/l)))
		} else {
			cah.outBuff = cah.outBuff[:16]
		}
		cah.nonce++
		binary.BigEndian.PutUint64(cah.outBuff[:8], cah.nonce)
		binary.BigEndian.PutUint64(cah.nonceAryOut[16:], cah.nonce)
		secretbox.Seal(cah.outBuff[16:], msg, cah.nonceAryOut, cah.sessionKey)
		binary.BigEndian.PutUint64(cah.outBuff[8:16], uint64(reqLen-16))
		_, err = cah.socket.Write(cah.outBuff[:reqLen])
	}
	return
}

func (cah *connectionAwaitHandshake) readAndDecryptOne() (*capn.Segment, error) {
	if cah.sessionKey == nil {
		return capn.ReadFromStream(cah.socket, nil)
	}
	read, err := cah.socket.Read(cah.inBuff)
	if err != nil {
		return nil, err
	} else if read < len(cah.inBuff) {
		return nil, fmt.Errorf("Only read %v bytes, wanted %v", read, len(cah.inBuff))
	}
	copy(cah.nonceAryIn[16:], cah.inBuff[:8])
	msgLen := binary.BigEndian.Uint64(cah.inBuff[8:16])
	plainLen := msgLen - secretbox.Overhead
	msgBuf := make([]byte, plainLen+msgLen)
	for recvBuf := msgBuf[plainLen:]; len(recvBuf) != 0; {
		read, err = cah.socket.Read(recvBuf)
		if err != nil {
			return nil, err
		} else {
			recvBuf = recvBuf[read:]
		}
	}
	plaintext, ok := secretbox.Open(msgBuf[:0], msgBuf[plainLen:], cah.nonceAryIn, cah.sessionKey)
	if !ok {
		return nil, fmt.Errorf("Unable to decrypt message")
	}
	seg, _, err := capn.ReadFromMemoryZeroCopy(plaintext)
	return seg, err
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
	seg := capn.NewBuffer(nil)
	hello := msgs.NewRootHelloFromClient(seg)
	hello.SetUsername(cash.username)
	hello.SetPassword(cash.password)
	cash.username = ""
	cash.password = nil

	buf := new(bytes.Buffer)
	if _, err := seg.WriteTo(buf); err != nil {
		return false, err
	}
	if err := cash.send(buf.Bytes()); err != nil {
		return false, err
	}

	if seg, err := cash.readAndDecryptOne(); err == nil {
		server := msgs.ReadRootHelloFromServer(seg)
		root := server.Root()
		if len(root.Id()) != common.KeyLen {
			return false, fmt.Errorf("Root object VarUUId is of wrong length!")
		}
		cash.lock.Lock()
		cash.rootVUUId = common.MakeVarUUId(root.Id())
		cash.namespace = make([]byte, common.KeyLen)
		copy(cash.namespace[8:], server.Namespace())
		cash.serverHost = server.LocalHost()
		cash.rmId = common.RMId(binary.BigEndian.Uint32(cash.namespace[16:20]))
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
func (cr *connectionRun) init(conn *Connection) {
	cr.Connection = conn
}

func (cr *connectionRun) start() (bool, error) {
	log.Printf("Connection established to %v (%v)\n", cr.serverHost, cr.rmId)

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

func (cr *connectionRun) handleMsgFromServer(msg *msgs.ClientMessage) error {
	if cr.currentState != cr {
		return nil
	}
	cr.missingBeats = 0
	switch which := msg.Which(); which {
	case msgs.CLIENTMESSAGE_HEARTBEAT:
		// do nothing
	case msgs.CLIENTMESSAGE_CLIENTTXNOUTCOME:
		outcome := msg.ClientTxnOutcome()
		return cr.handleTxnOutcome(&outcome)
	default:
		return fmt.Errorf("Received unexpected message from server: %v", which)
	}
	return nil
}

func (cr *connectionRun) submitTxn(txnMsg *connectionMsgTxn) error {
	if cr.currentState != cr {
		if !txnMsg.setOutcomeError(nil, nil, fmt.Errorf("Connection in wrong state: %v", cr.currentState)) {
			return fmt.Errorf("Live txn already closed")
		}
		return nil
	}
	if cr.liveTxn != nil {
		if !txnMsg.setOutcomeError(nil, nil, fmt.Errorf("Existing live txn")) {
			return fmt.Errorf("Live txn already closed")
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

func (cr *connectionRun) handleTxnOutcome(outcome *msgs.ClientTxnOutcome) error {
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
	if !cr.liveTxn.setOutcomeError(outcome, modifiedVars, err) {
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
		if cr.socket != nil {
			if err := cr.socket.Close(); err != nil {
				log.Println(err)
			}
		}
		cr.reader.terminated.Wait()
		cr.reader = nil
		cr.socket = nil
	} else if cr.socket != nil {
		if err := cr.socket.Close(); err != nil {
			log.Println(err)
		}
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

func (cb *connectionBeater) connectionMsgWitness() {}

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
			if seg, err := cr.readAndDecryptOne(); err == nil {
				msg := msgs.ReadRootClientMessage(seg)
				if !cr.enqueueQuery((*connectionReadMessage)(&msg)) {
					return
				}
			} else {
				cr.enqueueQuery(&connectionReadError{err})
				return
			}
		}
	}
}

type connectionReadMessage msgs.ClientMessage

func (crm *connectionReadMessage) connectionMsgWitness() {}

type connectionReadError struct {
	error
}

func (cre *connectionReadError) connectionMsgWitness() {}
