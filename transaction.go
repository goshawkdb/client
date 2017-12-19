package client

import (
	"errors"
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
	"sync"
)

type Transaction struct {
	parent     *Transaction
	connection *Connection
	rootCache  *cache

	roots map[string]*RefCap

	effects       map[common.VarUUId]*effect
	restartNeeded bool
	aborted       bool

	hasChild bool
	lock     *sync.Mutex
}

type Transactor interface {
	Transact(fun func(*Transaction) (interface{}, error)) (interface{}, error)
}

func runRootTxn(fun func(*Transaction) (interface{}, error), c *Connection, cache *cache, roots map[string]*RefCap) (interface{}, error) {
	txn := &Transaction{
		connection: c,
		rootCache:  cache,
		roots:      roots,
		lock:       new(sync.Mutex),
	}
	return txn.run(fun)
}

func (t *Transaction) Transact(fun func(*Transaction) (interface{}, error)) (interface{}, error) {
	t.lock.Lock()
	if err := t.valid(); err != nil {
		return nil, err
	}
	if t.hasChild {
		t.lock.Unlock()
		return nil, txnInProgress
	} else {
		t.hasChild = true
	}

	c := &Transaction{
		parent:     t,
		connection: t.connection,
		rootCache:  t.rootCache,
		roots:      t.roots,
		lock:       t.lock,
	}
	t.lock.Unlock()

	result, err := c.run(fun)

	t.lock.Lock()
	t.hasChild = false
	t.lock.Unlock()

	return result, err
}

func (t *Transaction) Root(name string) (RefCap, bool) {
	rc, found := t.roots[name]
	return *rc, found
}

func (t *Transaction) Abort() (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if err = t.valid(); err != nil {
		return
	}
	t.aborted = true
	return
}

func (t *Transaction) RestartNeeded() bool {
	t.lock.Lock()
	defer t.lock.Unlock()
	return t.restartNeeded
}

func (t *Transaction) Retry() (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if err = t.valid(); err != nil {
		return
	}
	reads := make(map[common.VarUUId]bool)
	for cur := t; cur != nil; cur = cur.parent {
		for vUUId, e := range cur.effects {
			if e.origRead {
				reads[vUUId] = true
			}
		}
	}
	DebugLog(t.connection.inner.Logger, "debug", "retry", "reads", reads)
	if len(reads) == 0 {
		return noReads
	}

	seg := capn.NewBuffer(nil)
	cTxn := msgs.NewClientTxn(seg)
	cTxn.SetRetry(true)
	actions := msgs.NewClientActionList(seg, len(reads))
	cTxn.SetActions(actions)
	idx := 0
	for vUUId := range reads {
		action := actions.At(idx)
		action.SetVarId(vUUId[:])
		action.SetUnmodified()
		action.SetActionType(msgs.CLIENTACTIONTYPE_READONLY)
		idx++
	}
	outcome, modifiedVars, err := t.connection.submitTransaction(&cTxn)
	if err != nil {
		return err
	}
	if outcome.Which() != msgs.CLIENTTXNOUTCOME_ABORT {
		panic("When retrying, failed to get abort outcome!")
	}
	t.determineRestart(modifiedVars)

	return
}

var (
	noReadCap     = errors.New("No capability to read.")
	noWriteCap    = errors.New("No capability to write.")
	aborted       = errors.New("Transaction has been aborted.")
	restartNeeded = errors.New("Restart needed.")
	noReads       = errors.New("Cannot retry: transaction never made any reads.")
	childExists   = errors.New("Illegal attempt to access parent transaction whilst child exists.")
)

func (t *Transaction) valid() error {
	if t.aborted {
		return aborted
	} else if t.restartNeeded {
		return restartNeeded
	} else if t.hasChild {
		return childExists
	} else {
		return nil
	}
}

func (t *Transaction) Read(ref RefCap) (value []byte, refs []RefCap, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if err = t.valid(); err != nil {
		return
	}
	e := t.find(ref.vUUId, true)
	if e.root != nil && !e.root.capability.CanRead() {
		return nil, nil, noReadCap
	}

	if e.curRefs == nil { // we need to load it
		vr, modifiedVars, err := t.load(ref.vUUId)
		if err != nil {
			return nil, nil, err
		}
		e.curValue = vr.value
		e.curRefs = vr.references
		t.determineRestart(modifiedVars)
		if t.restartNeeded {
			return nil, nil, nil
		}
	}

	t.effects[*ref.vUUId] = e
	// we read the original if we haven't written it yet, and we didn't create it
	e.origRead = e.origRead || (!e.origWritten && e.root != nil)

	value = make([]byte, len(e.curValue))
	copy(value, e.curValue)
	refs = make([]RefCap, len(e.curRefs))
	copy(refs, e.curRefs)
	return
}

func (t *Transaction) ObjectCapability(ref RefCap) (common.Capability, error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if err := t.valid(); err != nil {
		return common.NoneCapability, err
	}
	e := t.find(ref.vUUId, false)
	if e.root == nil {
		return common.ReadWriteCapability, nil
	} else {
		return e.root.capability, nil
	}
}

func (t *Transaction) Write(ref RefCap, value []byte, refs ...RefCap) (err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if err = t.valid(); err != nil {
		return
	}
	e := t.find(ref.vUUId, true)
	if e.root != nil && !e.root.capability.CanWrite() {
		return noWriteCap
	}

	t.effects[*ref.vUUId] = e
	// we wrote to the original if we haven't created it
	e.origWritten = e.origWritten || e.root != nil

	curValue := make([]byte, len(value))
	copy(curValue, value)
	curRefs := make([]RefCap, len(refs))
	copy(curRefs, refs)

	e.curValue = curValue
	e.curRefs = curRefs
	return
}

func (t *Transaction) Create(value []byte, refs ...RefCap) (ref RefCap, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	if err = t.valid(); err != nil {
		return
	}
	vUUId := t.connection.nextVarUUId()
	ref.vUUId = vUUId
	ref.capability = common.ReadWriteCapability

	curValue := make([]byte, len(value))
	copy(curValue, value)
	curRefs := make([]RefCap, len(refs))
	copy(curRefs, refs)

	e := &effect{
		curValue: curValue,
		curRefs:  curRefs,
	}
	t.effects[*vUUId] = e
	return
}

type effect struct {
	root        *valueRef // nb root will be nil if txn has created this fresh.
	curValue    []byte
	curRefs     []RefCap
	origRead    bool
	origWritten bool
}

func (e *effect) clone() *effect {
	c := *e
	return &c
}

func (e *effect) setRefs(seg *capn.Segment, setter func(msgs.ClientVarIdPos_List)) {
	refs := msgs.NewClientVarIdPosList(seg, len(e.curRefs))
	for idx, rc := range e.curRefs {
		ref := refs.At(idx)
		ref.SetVarId(rc.vUUId[:])
		ref.SetCapability(rc.capability.AsMsg())
	}
	setter(refs)
}

func (t *Transaction) find(vUUId *common.VarUUId, clone bool) *effect {
	if e, found := t.effects[*vUUId]; found {
		return e
	} else if t.parent != nil {
		e := t.parent.find(vUUId, false)
		if clone {
			e = e.clone()
		}
		return e
	} else if vr := t.rootCache.Get(vUUId); vr == nil {
		panic(fmt.Sprintf("Attempt to fetch unknown object: %v", vUUId))
	} else {
		e := &effect{
			root:     vr,
			curValue: vr.value,
			curRefs:  vr.references,
		}
		return e
	}
}

func (t *Transaction) run(fun func(*Transaction) (interface{}, error)) (result interface{}, err error) {
	t.lock.Lock()
	defer t.lock.Unlock()
	defer t.empty()

	for {
		t.restartNeeded = false
		t.effects = make(map[common.VarUUId]*effect, 16+len(t.effects))
		t.lock.Unlock()

		result, err = fun(t)

		DebugLog(t.connection.inner.Logger,
			"debug", "postRun", "result", result, "error", err,
			"aborted", t.aborted, "restartNeeded", t.restartNeeded, "nilParent", t.parent == nil)

		t.lock.Lock()
		switch {
		case err != nil || t.aborted:
			return
		case t.restartNeeded && (t.parent == nil || !t.parent.restartNeeded):
			continue
		case t.restartNeeded:
			return
		case t.parent == nil:
			if err = t.commitToServer(); err == nil && t.restartNeeded {
				continue
			} else {
				return
			}
		default:
			t.commitToParent()
			return
		}
	}
}

func (t *Transaction) commitToParent() {
	p := t.parent
	for vUUId, e := range t.effects {
		if pe, found := p.effects[vUUId]; found {
			// invariant: pe.root == e.root
			pe.curValue = e.curValue
			pe.curRefs = e.curRefs
			pe.origRead = pe.origRead || e.origRead
			pe.origWritten = pe.origWritten || e.origWritten
		} else {
			p.effects[vUUId] = e
		}
	}
}

func (t *Transaction) commitToServer() (err error) {
	// invariant: t.parent == nil
	if len(t.effects) == 0 {
		return nil
	}

	seg := capn.NewBuffer(nil)
	cTxn := msgs.NewClientTxn(seg)
	cTxn.SetRetry(false)
	actions := msgs.NewClientActionList(seg, len(t.effects))
	cTxn.SetActions(actions)
	idx := 0
	DebugLog(t.connection.inner.Logger, "debug", "creating submission.")

	for vUUId, e := range t.effects {
		action := actions.At(idx)
		idx++
		action.SetVarId(vUUId[:])
		setMod := true
		switch {
		case e.root == nil:
			DebugLog(t.connection.inner.Logger, "vUUId", vUUId, "action", "created")
			action.SetActionType(msgs.CLIENTACTIONTYPE_CREATE)
		case e.origRead && e.origWritten:
			DebugLog(t.connection.inner.Logger, "vUUId", vUUId, "action", "rw")
			action.SetActionType(msgs.CLIENTACTIONTYPE_READWRITE)
		case e.origRead:
			DebugLog(t.connection.inner.Logger, "vUUId", vUUId, "action", "read")
			action.SetActionType(msgs.CLIENTACTIONTYPE_READONLY)
			setMod = false
		case e.origWritten:
			DebugLog(t.connection.inner.Logger, "vUUId", vUUId, "action", "wrote")
			action.SetActionType(msgs.CLIENTACTIONTYPE_WRITEONLY)
		default:
			panic(fmt.Sprintf("Effect appears to be a noop! %v %#v", vUUId, e))
		}
		if setMod {
			action.SetModified()
			mod := action.Modified()
			mod.SetValue(e.curValue)
			e.setRefs(seg, mod.SetReferences)
		} else {
			action.SetUnmodified()
		}
	}
	DebugLog(t.connection.inner.Logger, "debug", "submitting")

	_, modifiedVars, err := t.connection.submitTransaction(&cTxn)
	if err != nil {
		return err
	}
	t.determineRestart(modifiedVars)

	return
}

func (t *Transaction) empty() {
	// wipe out all fields of t to force panics if it gets erroneously
	// reused.
	t.parent = nil
	t.connection = nil
	t.rootCache = nil
	t.roots = nil
	t.effects = nil
}

func (t *Transaction) determineRestart(modifiedVars []*common.VarUUId) {
	if t.parent == nil {
		for _, vUUId := range modifiedVars {
			if _, found := t.effects[*vUUId]; found {
				t.restartNeeded = true
				return
			}
		}
	} else {
		t.parent.determineRestart(modifiedVars)
		t.restartNeeded = t.parent.restartNeeded
		if !t.restartNeeded {
			for _, vUUId := range modifiedVars {
				if _, found := t.effects[*vUUId]; found {
					t.restartNeeded = true
					return
				}
			}
		}
	}
}

// the return modifiedVars does not contain vars that are new to us -
// only values that we already had *fully* loaded in the cache and
// that have now been modified (either updated or deleted).
func (t *Transaction) load(vUUId *common.VarUUId) (vr *valueRef, modifiedVars []*common.VarUUId, err error) {
	seg := capn.NewBuffer(nil)
	cTxn := msgs.NewClientTxn(seg)
	actions := msgs.NewClientActionList(seg, 1)
	cTxn.SetActions(actions)
	action := actions.At(0)
	action.SetVarId(vUUId[:])
	action.SetActionType(msgs.CLIENTACTIONTYPE_READONLY)
	action.SetUnmodified()
	outcome, modifiedVars, err := t.connection.submitTransaction(&cTxn)
	if err != nil {
		return
	}
	if outcome.Which() != msgs.CLIENTTXNOUTCOME_ABORT {
		origTxnId := common.MakeTxnId(outcome.Id())
		finalTxnId := common.MakeTxnId(outcome.FinalId())
		panic(fmt.Sprintf("%v (%v) When loading %v, failed to get abort outcome! %v", finalTxnId, origTxnId, vUUId, outcome.Which()))
	}
	vr = t.rootCache.Get(vUUId)
	return
}
