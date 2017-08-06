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
	mailbox     *actor.Mailbox          // enqueue msgs
	basic       *actor.BasicServerOuter // ExecFuncAsync
	lock        sync.RWMutex
	curTxn      *Txn
	nextVUUId   uint64
	namespace   []byte
	nextTxnId   uint64
	rootVUUIds  map[string]*refCap
	cache       *cache
	liveTxn     *connectionMsgTxn
	conn        *conn
	rng         *rand.Rand
	established chan error
}

type connectionInner struct {
	*actor.BasicServerInner // super-type, essentially
	*Connection             // need to access cache, namespace etc
	newConn                 func(*actor.BasicServerOuter, *actor.Mailbox) (*conn, error)
}

type connectionProtocol struct {
	*actor.BasicServerOuter // ExecFuncAsync
	*actor.Mailbox
	*Connection
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

	established := make(chan error)

	c := &Connection{
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
		established: established,
	}

	ci := &connectionInner{
		BasicServerInner: actor.NewBasicServerInner(logger),
		Connection:       c,
	}

	ci.newConn = func(outer *actor.BasicServerOuter, mailbox *actor.Mailbox) (*conn, error) {
		ci.newConn = nil
		cp := &connectionProtocol{
			BasicServerOuter: outer,
			Mailbox:          mailbox,
			Connection:       c,
		}
		return newConnTCPTLSCapnpDialer(cp, ci.Logger, hostPort, clientCertAndKeyPEM, clusterCertPEM)
	}

	mailbox, err := actor.Spawn(ci)
	if err != nil {
		return nil, err
	}
	c.mailbox = mailbox

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

// Run a transaction. The transaction is the function supplied, and
// the function is invoked potentially several times until it
// completes successfully: either committing or choosing to abort. The
// function should therefore be referentially transparent. Returning
// any non-nil error will cause the transaction to be aborted. The
// only exception to this rule is that returning Restart when the
// transaction has identified a restart is required will cause the
// transaction to be immediately restarted (methods on ObjectRef will
// return Restart as necessary).
//
// The function's final results are returned by this method, along
// with statistics regarding how the transaction proceeded.
//
// This function automatically detects and creates nested
// transactions: it is perfectly safe (and expected) to call
// RunTransaction from within a transaction.
func (c *Connection) RunTransaction(fun func(*Txn) (interface{}, error)) (interface{}, *Stats, error) {
	roots := c.rootVarUUIds()
	if roots == nil {
		return nil, nil, fmt.Errorf("Unable to start transaction: root objects not ready")
	}
	var oldTxn *Txn
	c.lock.Lock()
	txn := newTxn(fun, c, c.cache, roots, c.curTxn)
	c.curTxn, oldTxn = txn, c.curTxn
	c.lock.Unlock()
	res, stats, err := txn.run()
	c.lock.Lock()
	c.curTxn = oldTxn
	c.lock.Unlock()
	return res, stats, err
}

func (c *Connection) rootVarUUIds() map[string]*refCap {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.rootVUUIds
}

func (c *Connection) nextVarUUId() *common.VarUUId {
	c.lock.Lock()
	defer c.lock.Unlock()
	binary.BigEndian.PutUint64(c.namespace[:8], c.nextVUUId)
	vUUId := common.MakeVarUUId(c.namespace)
	c.nextVUUId++
	return vUUId
}

type connectionMsgTxn struct {
	actor.MsgSyncQuery
	txn          *msgs.ClientTxn
	err          error
	outcome      *msgs.ClientTxnOutcome
	modifiedVars []*common.VarUUId
	connection   *Connection
}

func (cmt *connectionMsgTxn) setOutcomeError(outcome *msgs.ClientTxnOutcome, modifiedVars []*common.VarUUId, err error) {
	cmt.outcome = outcome
	cmt.modifiedVars = modifiedVars
	cmt.err = err
	cmt.MustClose()
}

func (cmt *connectionMsgTxn) Exec() (bool, error) {
	c := cmt.connection
	if c.established != nil || c.conn == nil || !c.conn.IsRunning() {
		err := errors.New("Connection not ready")
		cmt.setOutcomeError(nil, nil, err)
		return false, nil
	}
	if c.liveTxn != nil {
		err := errors.New("Existing live txn")
		cmt.setOutcomeError(nil, nil, err)
		return false, nil
	}
	c.lock.Lock()
	binary.BigEndian.PutUint64(c.namespace[:8], c.nextTxnId)
	cmt.txn.SetId(c.namespace)
	c.lock.Unlock()
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootClientMessage(seg)
	msg.SetClientTxnSubmission(*cmt.txn)
	if err := c.conn.SendMessage(common.SegToBytes(seg)); err == nil {
		c.liveTxn = cmt
		return false, nil
	} else {
		c.nextTxnId++
		return false, err
	}
}

func (c *Connection) submitTransaction(txn *msgs.ClientTxn) (*msgs.ClientTxnOutcome, []*common.VarUUId, error) {
	container := &connectionMsgTxn{txn: txn, connection: c}
	container.InitMsg(c.mailbox)
	if c.mailbox.EnqueueMsg(container) && container.Wait() {
		return container.outcome, container.modifiedVars, container.err
	} else {
		return nil, nil, actor.MsgShutdown{}
	}
}

func (cp *connectionProtocol) Setup(roots map[string]*refCap, namespace []byte) {
	cp.EnqueueFuncAsync(func() (bool, error) {
		if cp.cache != nil {
			panic("handleSetup called twice.")
		}
		cp.lock.Lock()
		cp.rootVUUIds = roots
		cp.namespace = namespace
		cp.cache = newCache()
		cp.cache.SetRoots(roots)
		cp.lock.Unlock()
		if cp.established != nil {
			close(cp.established)
			cp.established = nil
		}
		return false, nil
	})
}

func (cp *connectionProtocol) TxnOutcome(outcome msgs.ClientTxnOutcome) {
	cp.EnqueueFuncAsync(func() (terminate bool, err error) {
		txnId := common.MakeTxnId(outcome.Id())
		if cp.liveTxn == nil {
			panic(fmt.Sprintf("Received txn outcome for unknown txn: %v", txnId))
		}
		finalTxnId := common.MakeTxnId(outcome.FinalId())
		if !bytes.Equal(cp.liveTxn.txn.Id(), outcome.Id()) {
			panic(fmt.Sprintf("Received txn outcome for wrong txn: %v (expecting %v) (final %v) (which %v)", txnId, common.MakeTxnId(cp.liveTxn.txn.Id()), finalTxnId, outcome.Which()))
		}
		final := binary.BigEndian.Uint64(finalTxnId[:8])
		if final < cp.nextTxnId {
			panic(fmt.Sprintf("Final (%v) < next (%v)\n", final, cp.nextTxnId))
		}
		cp.nextTxnId = final + 1 + uint64(cp.rng.Intn(8))
		var modifiedVars []*common.VarUUId
		switch outcome.Which() {
		case msgs.CLIENTTXNOUTCOME_COMMIT:
			cp.cache.updateFromTxnCommit(cp.liveTxn.txn, finalTxnId)
		case msgs.CLIENTTXNOUTCOME_ABORT:
			updates := outcome.Abort()
			modifiedVars = cp.cache.updateFromTxnAbort(&updates)
		case msgs.CLIENTTXNOUTCOME_ERROR:
			err = errors.New(outcome.Error())
		}
		cp.liveTxn.setOutcomeError(&outcome, modifiedVars, err)
		cp.liveTxn = nil
		return
	})
}

func (ci *connectionInner) Init(self *actor.Actor) (terminate bool, err error) {
	terminate, err = ci.BasicServerInner.Init(self)
	if terminate || err != nil {
		return
	}

	ci.basic = actor.NewBasicServerOuter(self.Mailbox)

	conn, err := ci.newConn(ci.basic, self.Mailbox)
	if err == nil {
		ci.conn = conn
		return false, ci.conn.Start()
	} else {
		return false, err
	}
}

func (ci *connectionInner) HandleShutdown(err error) bool {
	if ci.conn != nil {
		ci.conn.Shutdown()
		ci.conn = nil
	}
	if ci.established != nil {
		ci.established <- err
		close(ci.established)
		ci.established = nil
	}
	return ci.BasicServerInner.HandleShutdown(err)
}
