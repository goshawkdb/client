package client

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"github.com/go-kit/kit/log"
	cc "github.com/msackman/chancell"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Connection represents a connection to the GoshawkDB server. A
// connection may only be used by one go-routine at a time.
type Connection struct {
	sync.RWMutex
	curTxn            *Txn
	nextVUUId         uint64
	namespace         []byte
	rootVUUIds        map[string]*refCap
	cache             *cache
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(connectionMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	established       chan error
}

type connectionInner struct {
	*Connection
	liveTxn   *connectionMsgTxn
	nextTxnId uint64
	queryChan <-chan connectionMsg
	rng       *rand.Rand
	conn      *conn
	logger    log.Logger
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
		nextVUUId:   0,
		established: established,
	}

	ci := &connectionInner{
		Connection: c,
		nextTxnId:  0,
		rng:        rand.New(rand.NewSource(time.Now().UnixNano())),
		logger:     logger,
	}

	var head *cc.ChanCellHead
	head, c.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan connectionMsg, n)
			cell.Open = func() { ci.queryChan = queryChan }
			cell.Close = func() { close(queryChan) }
			c.enqueueQueryInner = func(msg connectionMsg, curCell *cc.ChanCell, cont cc.CurCellConsumer) (bool, cc.CurCellConsumer) {
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

	conn, err := newConnTCPTLSCapnpDialer(ci, logger, hostPort, clientCertAndKeyPEM, clusterCertPEM)
	if err != nil {
		return nil, err
	}
	ci.conn = conn

	if err = ci.conn.Start(); err != nil {
		return nil, err
	}

	go ci.actorLoop(head)

	if err = <-established; err == nil {
		logger.Log("msg", "Connection established.")
		return c, nil
	} else {
		return nil, err
	}
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
	c.Lock()
	txn := newTxn(fun, c, c.cache, roots, c.curTxn)
	c.curTxn, oldTxn = txn, c.curTxn
	c.Unlock()
	res, stats, err := txn.run()
	c.Lock()
	c.curTxn = oldTxn
	c.Unlock()
	return res, stats, err
}

func (c *Connection) rootVarUUIds() map[string]*refCap {
	c.RLock()
	defer c.RUnlock()
	return c.rootVUUIds
}

func (c *Connection) nextVarUUId() *common.VarUUId {
	c.Lock()
	defer c.Unlock()
	binary.BigEndian.PutUint64(c.namespace[:8], c.nextVUUId)
	vUUId := common.MakeVarUUId(c.namespace)
	c.nextVUUId++
	return vUUId
}

type connectionMsg interface{}

type connectionMsgSync interface {
	connectionMsg
	common.MsgSync
}

type connectionMsgShutdown struct{}

func (cms connectionMsgShutdown) Error() string {
	return "Connection Shutdown"
}

// Shutdown the connection. This will block until the connection is
// closed.
func (c *Connection) Shutdown() {
	if c.enqueueQuery(connectionMsgShutdown{}) {
		c.cellTail.Wait()
	}
}

type connectionMsgTxn struct {
	common.MsgSyncQuery
	err          error
	txn          *msgs.ClientTxn
	outcome      *msgs.ClientTxnOutcome
	modifiedVars []*common.VarUUId
}

func (cmt *connectionMsgTxn) setOutcomeError(outcome *msgs.ClientTxnOutcome, modifiedVars []*common.VarUUId, err error) {
	cmt.outcome = outcome
	cmt.modifiedVars = modifiedVars
	cmt.err = err
	if !cmt.Close() {
		panic("Cannot setOutcomeError as resultChan already closed!")
	}
}

func (c *Connection) submitTransaction(txn *msgs.ClientTxn) (*msgs.ClientTxnOutcome, []*common.VarUUId, error) {
	query := &connectionMsgTxn{txn: txn}
	if c.enqueueQuerySync(query) {
		return query.outcome, query.modifiedVars, query.err
	} else {
		return nil, nil, connectionMsgShutdown{}
	}
}

// for common/protocols.ConnectionActor
type connectionMsgExecFuncError func() error

func (c *connectionInner) EnqueueFuncError(fun func() error) bool {
	return c.enqueueQuery(connectionMsgExecFuncError(fun))
}

type connectionQueryCapture struct {
	c   *Connection
	msg connectionMsg
}

func (cqc *connectionQueryCapture) ccc(cell *cc.ChanCell) (bool, cc.CurCellConsumer) {
	return cqc.c.enqueueQueryInner(cqc.msg, cell, cqc.ccc)
}

func (c *Connection) enqueueQuery(msg connectionMsg) bool {
	cqc := &connectionQueryCapture{c: c, msg: msg}
	return c.cellTail.WithCell(cqc.ccc)
}

func (c *Connection) enqueueQuerySync(msg connectionMsgSync) bool {
	resultChan := msg.Init()
	if c.enqueueQuery(msg) {
		select {
		case <-resultChan:
			return true
		case <-c.cellTail.Terminated:
			return false
		}
	} else {
		return false
	}
}

// actor loop

func (ci *connectionInner) actorLoop(head *cc.ChanCellHead) {
	var (
		err       error
		queryChan <-chan connectionMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = ci.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	for !terminate {
		if msg, ok := <-queryChan; ok {
			terminate, err = ci.handleMsg(msg)
			terminate = terminate || err != nil
		} else {
			head.Next(queryCell, chanFun)
		}
	}
	ci.cellTail.Terminate()
	ci.handleShutdown(err)
}

func (ci *connectionInner) handleMsg(msg connectionMsg) (terminate bool, err error) {
	switch msgT := msg.(type) {
	case *connectionMsgTxn:
		err = ci.submitTxn(msgT)
	case connectionMsgExecFuncError:
		err = msgT()
	case connectionMsgShutdown:
		terminate = true
	default:
		panic(fmt.Sprintf("Received unexpected message: %#v", msgT))
	}
	return
}

func (ci *connectionInner) handleShutdown(err error) {
	if err != nil {
		ci.logger.Log("error", err)
	}
	if ci.conn != nil {
		ci.conn.Shutdown()
		ci.conn = nil
	}
	if ci.liveTxn != nil {
		ci.liveTxn.setOutcomeError(nil, nil, connectionMsgShutdown{})
		ci.liveTxn = nil
	}
	if ci.established != nil {
		ci.established <- err
		close(ci.established)
		ci.established = nil
	}
}

func (ci *connectionInner) handleSetup(roots map[string]*refCap, namespace []byte) error {
	if ci.cache != nil {
		panic("handleSetup called twice.")
	}
	ci.Lock()
	ci.rootVUUIds = roots
	ci.namespace = namespace
	ci.Unlock()
	ci.cache = newCache()
	ci.cache.SetRoots(roots)
	if ci.established != nil {
		close(ci.established)
		ci.established = nil
	}
	return nil
}

func (ci *connectionInner) submitTxn(txnMsg *connectionMsgTxn) error {
	if ci.established != nil || ci.conn == nil || !ci.conn.IsRunning() {
		err := errors.New("Connection not ready")
		txnMsg.setOutcomeError(nil, nil, err)
		return nil
	}
	if ci.liveTxn != nil {
		err := errors.New("Existing live txn")
		txnMsg.setOutcomeError(nil, nil, err)
		return nil
	}
	binary.BigEndian.PutUint64(ci.namespace[:8], ci.nextTxnId)
	txnMsg.txn.SetId(ci.namespace)
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootClientMessage(seg)
	msg.SetClientTxnSubmission(*txnMsg.txn)
	if err := ci.conn.SendMessage(common.SegToBytes(seg)); err == nil {
		ci.liveTxn = txnMsg
		return nil
	} else {
		ci.nextTxnId++
		return err
	}
}

func (ci *connectionInner) handleTxnOutcome(outcome msgs.ClientTxnOutcome) error {
	txnId := common.MakeTxnId(outcome.Id())
	if ci.liveTxn == nil {
		panic(fmt.Sprintf("Received txn outcome for unknown txn: %v", txnId))
	}
	finalTxnId := common.MakeTxnId(outcome.FinalId())
	if !bytes.Equal(ci.liveTxn.txn.Id(), outcome.Id()) {
		panic(fmt.Sprintf("Received txn outcome for wrong txn: %v (expecting %v) (final %v) (which %v)", txnId, common.MakeTxnId(ci.liveTxn.txn.Id()), finalTxnId, outcome.Which()))
	}
	var err error
	final := binary.BigEndian.Uint64(finalTxnId[:8])
	if final < ci.nextTxnId {
		panic(fmt.Sprintf("Final (%v) < next (%v)\n", final, ci.nextTxnId))
	}
	ci.nextTxnId = final + 1 + uint64(ci.rng.Intn(8))
	var modifiedVars []*common.VarUUId
	switch outcome.Which() {
	case msgs.CLIENTTXNOUTCOME_COMMIT:
		ci.cache.updateFromTxnCommit(ci.liveTxn.txn, finalTxnId)
	case msgs.CLIENTTXNOUTCOME_ABORT:
		updates := outcome.Abort()
		modifiedVars = ci.cache.updateFromTxnAbort(&updates)
	case msgs.CLIENTTXNOUTCOME_ERROR:
		err = errors.New(outcome.Error())
	}
	ci.liveTxn.setOutcomeError(&outcome, modifiedVars, err)
	ci.liveTxn = nil
	return nil
}
