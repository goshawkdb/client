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
	liveTxn           *connectionMsgTxn
	nextVUUId         uint64
	nextTxnId         uint64
	namespace         []byte
	rootVUUIds        map[string]*refCap
	cache             *cache
	cellTail          *cc.ChanCellTail
	enqueueQueryInner func(connectionMsg, *cc.ChanCell, cc.CurCellConsumer) (bool, cc.CurCellConsumer)
	queryChan         <-chan connectionMsg
	rng               *rand.Rand
	conn              *conn
	logger            log.Logger
	established       chan struct{}
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

	established := make(chan struct{})

	c := &Connection{
		nextVUUId:   0,
		nextTxnId:   0,
		rng:         rand.New(rand.NewSource(time.Now().UnixNano())),
		logger:      logger,
		established: established,
	}

	var head *cc.ChanCellHead
	head, c.cellTail = cc.NewChanCellTail(
		func(n int, cell *cc.ChanCell) {
			queryChan := make(chan connectionMsg, n)
			cell.Open = func() { c.queryChan = queryChan }
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

	conn, err := newConnTCPTLSCapnpDialer(c, logger, hostPort, clientCertAndKeyPEM, clusterCertPEM)
	if err != nil {
		return nil, err
	}
	c.conn = conn

	if err = c.conn.Start(); err != nil {
		return nil, err
	}

	go c.actorLoop(head)

	<-established

	return c, nil
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
func (c *Connection) Shutdown() {
	if c.enqueueQuery(connectionMsgShutdown{}) {
		c.cellTail.Wait()
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

func (c *Connection) submitTransaction(txn *msgs.ClientTxn) (*msgs.ClientTxnOutcome, []*common.VarUUId, error) {
	query := &connectionMsgTxn{txn: txn}
	query.init()
	if c.enqueueSyncQuery(query, query.resultChan) {
		return query.outcome, query.modifiedVars, query.err
	} else {
		return nil, nil, connectionMsgShutdown{}
	}
}

type connectionMsgExecError func() error

func (cmee connectionMsgExecError) witness() connectionMsg { return cmee }

func (c *Connection) EnqueueError(fun func() error) bool {
	return c.enqueueQuery(connectionMsgExecError(fun))
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

func (c *Connection) enqueueSyncQuery(msg connectionMsg, resultChan chan struct{}) bool {
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

func (c *Connection) actorLoop(head *cc.ChanCellHead) {
	var (
		err       error
		queryChan <-chan connectionMsg
		queryCell *cc.ChanCell
	)
	chanFun := func(cell *cc.ChanCell) { queryChan, queryCell = c.queryChan, cell }
	head.WithCell(chanFun)
	terminate := false
	for !terminate {
		if msg, ok := <-queryChan; ok {
			terminate, err = c.handleMsg(msg)
		} else {
			head.Next(queryCell, chanFun)
		}
		terminate = terminate || err != nil
	}
	c.cellTail.Terminate()
	c.handleShutdown(err)
}

func (c *Connection) handleMsg(msg connectionMsg) (terminate bool, err error) {
	switch msgT := msg.(type) {
	case *connectionMsgTxn:
		err = c.submitTxn(msgT)
	case connectionMsgExecError:
		err = msgT()
	case connectionMsgShutdown:
		terminate = true
	default:
		err = fmt.Errorf("Received unexpected message: %v", msgT)
	}
	return
}

func (c *Connection) handleShutdown(err error) {
	if err != nil {
		c.logger.Log("error", err)
	}
	if c.conn != nil {
		c.conn.Shutdown()
		c.conn = nil
	}
	e := fmt.Errorf("Connection shutting down")
	if c.liveTxn != nil {
		if !c.liveTxn.setOutcomeError(nil, nil, e) {
			panic("Internal Logic Failure when closing outstanding live txn")
		}
		c.liveTxn = nil
	}
	if c.established != nil {
		close(c.established)
		c.established = nil
	}
}

func (c *Connection) handleSetup(roots map[string]*refCap, namespace []byte) error {
	if c.cache != nil {
		panic("handleSetup called twice.")
	}
	c.Lock()
	c.rootVUUIds = roots
	c.namespace = namespace
	c.Unlock()
	c.cache = newCache()
	c.cache.SetRoots(roots)
	if c.established != nil {
		close(c.established)
		c.established = nil
	}
	return nil
}

func (c *Connection) submitTxn(txnMsg *connectionMsgTxn) error {
	if c.established != nil || c.conn == nil || !c.conn.IsRunning() {
		err := errors.New("Connection not ready")
		if !txnMsg.setOutcomeError(nil, nil, err) {
			return err
		}
		return nil
	}
	if c.liveTxn != nil {
		err := errors.New("Existing live txn")
		if !txnMsg.setOutcomeError(nil, nil, err) {
			return err
		}
		return nil
	}
	binary.BigEndian.PutUint64(c.namespace[:8], c.nextTxnId)
	txnMsg.txn.SetId(c.namespace)
	seg := capn.NewBuffer(nil)
	msg := msgs.NewRootClientMessage(seg)
	msg.SetClientTxnSubmission(*txnMsg.txn)
	if err := c.conn.SendMessage(common.SegToBytes(seg)); err == nil {
		c.liveTxn = txnMsg
		return nil
	} else {
		c.nextTxnId++
		return err
	}
}

func (c *Connection) handleTxnOutcome(outcome msgs.ClientTxnOutcome) error {
	txnId := common.MakeTxnId(outcome.Id())
	if c.liveTxn == nil {
		return fmt.Errorf("Received txn outcome for unknown txn: %v", txnId)
	}
	finalTxnId := common.MakeTxnId(outcome.FinalId())
	if !bytes.Equal(c.liveTxn.txn.Id(), outcome.Id()) {
		return fmt.Errorf("Received txn outcome for wrong txn: %v (expecting %v) (final %v) (which %v)", txnId, common.MakeTxnId(c.liveTxn.txn.Id()), finalTxnId, outcome.Which())
	}
	var err error
	final := binary.BigEndian.Uint64(finalTxnId[:8])
	if final < c.nextTxnId {
		return fmt.Errorf("Final (%v) < next (%v)\n", final, c.nextTxnId)
	}
	c.nextTxnId = final + 1 + uint64(c.rng.Intn(8))
	var modifiedVars []*common.VarUUId
	switch outcome.Which() {
	case msgs.CLIENTTXNOUTCOME_COMMIT:
		c.cache.updateFromTxnCommit(c.liveTxn.txn, finalTxnId)
	case msgs.CLIENTTXNOUTCOME_ABORT:
		updates := outcome.Abort()
		modifiedVars = c.cache.updateFromTxnAbort(&updates)
	case msgs.CLIENTTXNOUTCOME_ERROR:
		err = errors.New(outcome.Error())
	}
	if !c.liveTxn.setOutcomeError(&outcome, modifiedVars, err) {
		return errors.New("Live txn already closed")
	}
	c.liveTxn = nil
	return nil
}
