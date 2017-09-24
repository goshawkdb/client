package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	"goshawkdb.io/common/actor"
	msgs "goshawkdb.io/common/capnp"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Connection represents a connection to the GoshawkDB server. A
// connection may only be used by one go-routine at a time.
type Connection struct {
	mailbox *actor.Mailbox          // enqueue msgs
	basic   *actor.BasicServerOuter // ExecFuncAsync

	lock      sync.Mutex // the lock is for hasTxn, nextVUUId, namespace and cache
	hasTxn    bool
	nextVUUId uint64
	namespace []byte
	cache     *cache

	nextTxnId uint64
	roots     map[string]*RefCap

	txnMsg      *connectionMsgTxn
	conn        *conn
	rng         *rand.Rand
	established chan error

	submittedCount   uint64
	committedCount   uint64
	restartedCount   uint64
	sumServerLatency time.Duration

	inner connectionInner
	proto connectionProtocol
}

type connectionInner struct {
	*Connection             // need to access cache, namespace etc
	*actor.BasicServerInner // super-type, essentially
}

type connectionProtocol struct {
	*Connection
	*actor.Mailbox
	*actor.BasicServerOuter // ExecFuncAsync
}

func (c *Connection) nextVarUUId() *common.VarUUId {
	c.lock.Lock()
	defer c.lock.Unlock()
	binary.BigEndian.PutUint64(c.namespace[:8], c.nextVUUId)
	vUUId := common.MakeVarUUId(c.namespace)
	c.nextVUUId++
	return vUUId
}

// Create a new connection. The hostPort parameter can be either
// hostname:port or ip:port. If port is not provided the default port
// of 7894 is used. This will block until a connection is established
// and ready to use, or an error occurs. The clientCertAndKeyPEM
// parameter should be the client certificate followed by private key
// in PEM format. The clusterCert parameter is the cluster
// certificate. This is optional, but recommended: without it, the
// client will not be able to verify the server to which it connects.
func NewConnection(hostPort string, clientCertAndKeyPEM, clusterCertPEM []byte, logger log.Logger) (*Connection, error) {
	if logger == nil {
		logger = log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
		logger = log.With(logger, "ts", log.DefaultTimestampUTC)
	}
	logger = log.With(logger, "remoteHost", hostPort)

	established := make(chan error)

	c := &Connection{
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
		established: established,
	}

	ci := &c.inner
	ci.Connection = c
	ci.BasicServerInner = actor.NewBasicServerInner(logger)

	mailbox, err := actor.Spawn(ci)
	if err != nil {
		panic(err) // "impossible"
	}

	cp := &c.proto
	cp.Connection = c
	cp.Mailbox = mailbox
	cp.BasicServerOuter = c.basic

	cp.EnqueueFuncAsync(func() (bool, error) {
		conn, err := newConnTCPTLSCapnpDialer(cp, ci.Logger, hostPort, clientCertAndKeyPEM, clusterCertPEM)
		if err == nil {
			c.conn = conn
			return false, conn.Start()
		} else {
			return false, err
		}
	})

	if err = <-established; err == nil {
		logger.Log("msg", "Connection established.")
		return c, nil
	} else {
		return nil, err
	}
}

// Shutdown the connection, closing the socket and releasing all
// resources. This method blocks until the socket is closed and
// resources are released. Any transaction which is currently in the
// process of being submitted to the server is killed off on the
// client side: in this case, it is not possible to know whether or
// not the server committed such a transaction or not.
func (c *Connection) ShutdownSync() {
	c.basic.ShutdownSync()
}

var txnInProgress = errors.New("Transaction already in progress. Use txn.Transact to start a nested txn.")

func (c *Connection) Transact(fun func(*Transaction) (interface{}, error)) (interface{}, error) {
	c.lock.Lock()
	if c.hasTxn {
		c.lock.Unlock()
		return nil, txnInProgress
	} else {
		c.hasTxn = true
	}

	cache := c.cache
	roots := c.roots
	c.lock.Unlock()

	result, err := runRootTxn(fun, c, cache, roots)

	c.lock.Lock()
	c.hasTxn = false
	c.lock.Unlock()

	return result, err
}

// submitting a txn
type connectionMsgTxn struct {
	actor.MsgSyncQuery
	c            *Connection
	started      time.Time
	txn          *msgs.ClientTxn
	outcome      *msgs.ClientTxnOutcome
	modifiedVars []*common.VarUUId
	err          error
}

func (msg *connectionMsgTxn) setOutcomeError(outcome *msgs.ClientTxnOutcome, modifiedVars []*common.VarUUId, err error) {
	msg.outcome = outcome
	msg.modifiedVars = modifiedVars
	msg.err = err
	msg.MustClose()
}

func (msg *connectionMsgTxn) Exec() (bool, error) {
	c := msg.c
	if c.established != nil || c.conn == nil || !c.conn.IsRunning() {
		msg.setOutcomeError(nil, nil, errors.New("Connection not ready"))
		return false, nil
	}
	if c.txnMsg != nil {
		panic("Should be impossible: existing txnMsg found.")
	}
	started := time.Now()
	txn := msg.txn
	c.lock.Lock()
	binary.BigEndian.PutUint64(c.namespace[:8], c.nextTxnId)
	txn.SetId(c.namespace)
	c.lock.Unlock()
	seg := capn.NewBuffer(nil)
	txnCap := msgs.NewRootClientMessage(seg)
	txnCap.SetClientTxnSubmission(*txn)
	if err := c.conn.SendMessage(common.SegToBytes(seg)); err == nil {
		c.submittedCount++
		c.txnMsg = msg
		msg.started = started
		return false, nil
	} else {
		msg.setOutcomeError(nil, nil, err)
		c.nextTxnId++
		return false, err
	}
}

func (c *Connection) submitTransaction(txn *msgs.ClientTxn) (*msgs.ClientTxnOutcome, []*common.VarUUId, error) {
	msg := &connectionMsgTxn{c: c, txn: txn}
	msg.InitMsg(c.mailbox)
	if c.mailbox.EnqueueMsg(msg) && msg.Wait() {
		return msg.outcome, msg.modifiedVars, msg.err
	} else {
		return nil, nil, actor.MsgShutdown{}
	}
}

// setup callback from protocol
type connectionMsgSetup struct {
	c         *Connection
	roots     map[string]*RefCap
	namespace []byte
}

func (msg *connectionMsgSetup) Exec() (bool, error) {
	c := msg.c
	if c.cache != nil {
		panic("Setup called twice.")
	}
	c.lock.Lock()
	c.roots = msg.roots
	c.namespace = msg.namespace
	c.cache = newCache(msg.roots, c.inner.Logger)
	c.lock.Unlock()
	if c.established != nil {
		close(c.established)
		c.established = nil
	}
	return false, nil
}

func (cp *connectionProtocol) Setup(roots map[string]*RefCap, namespace []byte) {
	cp.EnqueueMsg(&connectionMsgSetup{c: cp.Connection, roots: roots, namespace: namespace})
}

// txn outcome received callback from protocol

type connectionMsgTxnOutcome struct {
	c       *Connection
	outcome msgs.ClientTxnOutcome
}

func (msg *connectionMsgTxnOutcome) Exec() (bool, error) {
	c := msg.c
	outcome := msg.outcome
	txnId := common.MakeTxnId(outcome.Id())
	if c.txnMsg == nil {
		panic(fmt.Sprintf("Received txn outcome for unknown txn: %v", txnId))
	}

	finalTxnId := common.MakeTxnId(outcome.FinalId())
	if !bytes.Equal(c.txnMsg.txn.Id(), outcome.Id()) {
		panic(fmt.Sprintf("Received txn outcome for wrong txn: %v (expecting %v) (final %v) (which %v)", txnId, common.MakeTxnId(c.txnMsg.txn.Id()), finalTxnId, outcome.Which()))
	}
	final := binary.BigEndian.Uint64(finalTxnId[:8])
	if final < c.nextTxnId {
		panic(fmt.Sprintf("Final (%v) < next (%v)\n", final, c.nextTxnId))
	}
	c.nextTxnId = final + 1 + uint64(c.rng.Intn(8))

	c.sumServerLatency += time.Now().Sub(c.txnMsg.started)

	var err error
	var modifiedVars []*common.VarUUId
	switch outcome.Which() {
	case msgs.CLIENTTXNOUTCOME_COMMIT:
		c.committedCount++
		c.cache.updateFromTxnCommit(c.txnMsg.txn)
	case msgs.CLIENTTXNOUTCOME_ABORT:
		c.restartedCount++
		updates := outcome.Abort()
		modifiedVars = c.cache.updateFromTxnAbort(&updates)
	case msgs.CLIENTTXNOUTCOME_ERROR:
		err = errors.New(outcome.Error())
	default:
		panic(fmt.Sprintf("Unexpected txn outcome type! %v", outcome.Which()))
	}
	c.txnMsg.setOutcomeError(&outcome, modifiedVars, err)
	c.txnMsg = nil
	return false, nil
}

func (cp *connectionProtocol) TxnOutcome(outcome msgs.ClientTxnOutcome) {
	cp.EnqueueMsg(&connectionMsgTxnOutcome{c: cp.Connection, outcome: outcome})
}

// actory things
func (ci *connectionInner) Init(self *actor.Actor) (bool, error) {
	terminate, err := ci.BasicServerInner.Init(self)
	if terminate || err != nil {
		return terminate, err
	}

	ci.mailbox = self.Mailbox
	ci.basic = actor.NewBasicServerOuter(self.Mailbox)

	return false, nil
}

func (ci *connectionInner) HandleShutdown(err error) bool {
	if ci.submittedCount == 0 {
		ci.Logger.Log(
			"submitted", ci.submittedCount,
			"committed", ci.committedCount,
			"restarted", ci.restartedCount)
	} else {
		ci.Logger.Log(
			"submitted", ci.submittedCount,
			"committed", ci.committedCount,
			"restarted", ci.restartedCount,
			"avgLatency", ci.sumServerLatency/time.Duration(ci.submittedCount))
	}

	if ci.conn != nil {
		ci.conn.Shutdown()
		ci.conn = nil
	}

	errShutdown := err
	if err == nil {
		errShutdown = actor.MsgShutdown{}
	}

	if ci.established != nil {
		ci.established <- errShutdown
		close(ci.established)
		ci.established = nil
	}

	if ci.txnMsg != nil {
		ci.txnMsg.setOutcomeError(nil, nil, errShutdown)
		ci.txnMsg = nil
	}
	return ci.BasicServerInner.HandleShutdown(err)
}
