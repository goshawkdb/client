package client

import (
	"fmt"
	capn "github.com/glycerine/go-capnproto"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
	"time"
)

// A Txn object holds the state of the current transaction and is
// supplied to your transaction functions. Through this object you can
// interact with the GoshawkDB object store.
type Txn struct {
	fun             func(*Txn) (interface{}, error)
	conn            *Connection
	cache           *cache
	parent          *Txn
	roots           map[string]*refCap
	objs            map[common.VarUUId]*Object
	resetInProgress bool
	stats           *Stats
}

// A Stats object is created for each root transaction and shared with
// any nested transactions. It records some details about how the
// transaction progressed.
type Stats struct {
	// Any objects that were loaded from the RM as part of this transaction are recorded
	Loads map[common.VarUUId]time.Duration
	// Every time the local transaction payload is submitted to the RM
	// for validation and possible commitment, the time it took to
	// perform the submission is added here. Thus as a single
	// transaction may be submitted multiple times, so there may be
	// multiple elements in this list. Nested transactions never appear
	// here as nested transactions are client-side only.
	Submissions []time.Duration
	// The id of the transaction.
	TxnId *common.TxnId
}

type TxnFunResult int

const (
	// If you return Retry as the result (not error) of a transaction
	// then a retry transaction is performed.
	Retry TxnFunResult = iota
	// If the transaction detects that it needs to be restarted as soon
	// as possible then all methods on Object will return Restart as an
	// error. You should detect this and return Restart as an error in
	// the transaction function, allowing the transaction function to
	// be restarted promptly.
	Restart TxnFunResult = iota
)

func (tfr TxnFunResult) Error() string {
	switch tfr {
	case Retry:
		return "Retry"
	case Restart:
		return "Restart"
	default:
		panic(fmt.Sprintf("Unexpected TxnFunResult value: %v", tfr))
	}
}

func newTxn(fun func(*Txn) (interface{}, error), conn *Connection, cache *cache, roots map[string]*refCap, parent *Txn) *Txn {
	t := &Txn{
		fun:    fun,
		conn:   conn,
		cache:  cache,
		parent: parent,
		roots:  roots,
		objs:   make(map[common.VarUUId]*Object),
	}
	if parent == nil {
		t.stats = &Stats{
			Loads:       make(map[common.VarUUId]time.Duration),
			Submissions: []time.Duration{},
		}
		t.resetInProgress = false
	} else {
		t.stats = parent.stats
		t.resetInProgress = parent.resetInProgress
	}
	return t
}

func (txn *Txn) run() (interface{}, *Stats, error) {
	defer txn.resetObjects()

	for {
		if txn.resetInProgress {
			if txn.parent == nil || !txn.parent.resetInProgress {
				txn.resetInProgress = false
			} else {
				// log.Printf("%v refusing to start txn as resetInProgress\n", txn)
				return nil, txn.stats, Restart
			}
		}
		txn.resetObjects()
		// log.Printf("%v starting fun\n", txn)
		result, err := txn.fun(txn)
		// log.Printf("%v finished fun\n", txn)

		switch {
		case err != nil && err != Restart:
			// log.Printf("%v 1 returning %v %v\n", txn, result, err)
			return nil, txn.stats, err
		case txn.resetInProgress:
			if txn.parent == nil || !txn.parent.resetInProgress {
				// log.Printf("%v 2 resetInProgress, continuing\n", txn)
				continue
			} else {
				// log.Printf("%v 3 resetInProgress, returning %v\n", txn, result)
				return nil, txn.stats, Restart
			}
		case result == Retry:
			err = txn.submitRetryTransaction()
			switch {
			case err != nil:
				// log.Printf("%v 4 retry, returning err %v\n", txn, err)
				return nil, txn.stats, err
			case txn.parent == nil:
				// log.Printf("%v 5 retry, continuing\n", txn)
				continue
			default:
				// log.Printf("%v 6 retry, returning\n", txn)
				return nil, txn.stats, Restart
			}
		case txn.parent == nil:
			// log.Printf("%v 7 submitting to server\n", txn)
			start := time.Now()
			rerun, err := txn.submitToServer()
			txn.stats.Submissions = append(txn.stats.Submissions, time.Now().Sub(start))
			switch {
			case err != nil:
				// log.Printf("%v 8 returning err %v\n", txn, err)
				return nil, txn.stats, err
			case rerun:
				// log.Printf("%v 9 continuing rerun\n", txn)
				continue
			default:
				// log.Printf("%v 10 returning success %v\n", txn, result)
				return result, txn.stats, nil
			}
		default:
			// log.Printf("%v 11 moving to parent\n", txn)
			txn.moveObjsToParent()
			// log.Printf("%v 11 returning %v\n", txn, result)
			return result, txn.stats, nil
		}
	}
}

func (txn *Txn) resetObjects() {
	for vUUId, obj := range txn.objs {
		if obj.state.txn == txn {
			// log.Printf("%v resetting %v\n", txn, obj.Id)
			obj.state = obj.state.parentState
		}
		// log.Printf("%v deleting %v\n", txn, obj.Id)
		delete(txn.objs, vUUId)
	}
}

func (txn *Txn) submitRetryTransaction() error {
	reads := make(map[common.VarUUId]*objectState)
	for ancestor := txn; ancestor != nil; ancestor = ancestor.parent {
		for _, obj := range ancestor.objs {
			if _, found := reads[*obj.Id]; !found && obj.state.txn == ancestor && obj.state.read {
				reads[*obj.Id] = obj.state
			}
		}
	}
	seg := capn.NewBuffer(nil)
	cTxn := msgs.NewClientTxn(seg)
	cTxn.SetRetry(true)
	actions := msgs.NewClientActionList(seg, len(reads))
	cTxn.SetActions(actions)
	idx := 0
	for _, state := range reads {
		action := actions.At(idx)
		action.SetVarId(state.Id[:])
		action.SetRead()
		action.Read().SetVersion(state.curVersion[:])
		idx++
	}
	outcome, _, err := txn.conn.submitTransaction(&cTxn)
	if err != nil {
		return err
	}
	txn.stats.TxnId = common.MakeTxnId(outcome.FinalId())
	for ancestor := txn; ancestor != nil; ancestor = ancestor.parent {
		ancestor.resetInProgress = true
	}
	return nil
}

func (txn *Txn) moveObjsToParent() {
	parent := txn.parent
	objs := parent.objs
	for _, obj := range txn.objs {
		state := obj.state
		if obj.state.txn == txn {
			state.txn = parent
			if state.parentState != nil && state.parentState.txn == parent {
				state.parentState = state.parentState.parentState
			}
			// log.Printf("%v Set %v state[%p] to %v\n", txn, obj.Id, parent, state)
			if _, found := objs[*obj.Id]; !found {
				// log.Printf("%v added to parent objs %v\n", txn, obj.Id)
				objs[*obj.Id] = obj
			}
		}
	}
}

func (txn *Txn) varsUpdated(vUUIds []*common.VarUUId) bool {
	switch {
	case txn.parent != nil && txn.parent.varsUpdated(vUUIds):
		txn.resetInProgress = true
		return true
	case txn.resetInProgress:
		return true
	default:
		for _, vUUId := range vUUIds {
			if obj, found := txn.objs[*vUUId]; found && obj.state.txn == txn && obj.state.read {
				txn.resetInProgress = true
				return true
			}
		}
		return false
	}
}

func (txn *Txn) submitToServer() (bool, error) {
	// log.Println(txn, "Submitting to conn")
	reads := make([]*objectState, 0, len(txn.objs))
	writes := make([]*objectState, 0, len(txn.objs))
	readwrites := make([]*objectState, 0, len(txn.objs))
	creates := make([]*objectState, 0, len(txn.objs))
	for _, obj := range txn.objs {
		state := obj.state
		switch {
		case state.create:
			creates = append(creates, state)
		case state.read && state.write:
			readwrites = append(readwrites, state)
		case state.write:
			writes = append(writes, state)
		case state.read:
			reads = append(reads, state)
		}
	}

	totalLen := len(reads) + len(writes) + len(readwrites) + len(creates)
	if totalLen == 0 {
		return false, nil
	}

	// log.Printf("%v r:%v; w:%v; rw:%v; c%v; ", txn, len(reads), len(writes), len(readwrites), len(creates))

	total := make([]*objectState, totalLen)
	copy(total, reads)
	writeThresh := len(reads)
	copy(total[writeThresh:], writes)
	readwriteThresh := writeThresh + len(writes)
	copy(total[readwriteThresh:], readwrites)
	createThresh := readwriteThresh + len(readwrites)
	copy(total[createThresh:], creates)

	seg := capn.NewBuffer(nil)
	cTxn := msgs.NewClientTxn(seg)
	cTxn.SetRetry(false)
	actions := msgs.NewClientActionList(seg, totalLen)
	cTxn.SetActions(actions)
	idx := 0
	for _, state := range total {
		action := actions.At(idx)
		action.SetVarId(state.Id[:])
		if idx < writeThresh {
			action.SetRead()
			action.Read().SetVersion(state.curVersion[:])
		} else {
			refs := msgs.NewClientVarIdPosList(seg, len(state.curObjectRefs))
			for idy, ocp := range state.curObjectRefs {
				ref := refs.At(idy)
				ref.SetVarId(ocp.Object.Id[:])
				ref.SetCapability(ocp.capability.Capability)
			}
			switch {
			case idx < readwriteThresh:
				action.SetWrite()
				write := action.Write()
				write.SetValue(state.curValue)
				write.SetReferences(refs)
			case idx < createThresh:
				action.SetReadwrite()
				rw := action.Readwrite()
				rw.SetVersion(state.curVersion[:])
				rw.SetValue(state.curValue)
				rw.SetReferences(refs)
			default:
				action.SetCreate()
				create := action.Create()
				create.SetValue(state.curValue)
				create.SetReferences(refs)
			}
		}
		idx++
	}
	outcome, _, err := txn.conn.submitTransaction(&cTxn)
	if err != nil {
		return false, err
	}
	txn.stats.TxnId = common.MakeTxnId(outcome.FinalId())
	return outcome.Which() == msgs.CLIENTTXNOUTCOME_ABORT, nil
}

// Returns the database Root Objects. The Root Objects for each client
// are defined by the cluster configuration represent the roots of the
// object graphs. For an object to be reachable, there must be a path
// to it from a Root Object. If an error is returned, the current
// transaction should immediately be restarted (return the error
// Restart)
func (txn *Txn) GetRootObjects() (map[string]ObjectCapabilityPair, error) {
	roots := make(map[string]ObjectCapabilityPair, len(txn.roots))
	for name, rc := range txn.roots {
		if obj, err := txn.GetObject(rc.vUUId); err == nil {
			roots[name] = ObjectCapabilityPair{
				Object:     obj,
				capability: rc.capability,
			}
		} else {
			return nil, err
		}
	}
	return roots, nil
}

// Create a new object and set its value and references. If an error
// is returned, the current transaction should immediately be
// restarted (return the error Restart). This method takes copies of
// both the value and the references so if you modify either after
// calling this method, your modifications will not take effect.
func (txn *Txn) CreateObject(value []byte, references ...ObjectCapabilityPair) (*Object, error) {
	if txn.resetInProgress {
		return nil, Restart
	}

	obj := &Object{
		Id:   txn.conn.nextVarUUId(),
		conn: txn.conn,
	}
	txn.objs[*obj.Id] = obj
	state := &objectState{
		Object:        obj,
		parentState:   nil,
		txn:           txn,
		curValue:      make([]byte, len(value)),
		curObjectRefs: make([]ObjectCapabilityPair, len(references)),
		create:        true,
	}
	copy(state.curValue, value)
	copy(state.curObjectRefs, references)

	obj.state = state
	return obj, nil
}

// Fetches the object specified by its unique object id. Note this
// will fail unless the client has already navigated the object graph
// at least as far as any object that has a reference to the object
// id. This method is not normally necessary: it is generally
// preferred to use the References of objects to navigate.
func (txn *Txn) GetObject(vUUId *common.VarUUId) (*Object, error) {
	if txn.resetInProgress {
		return nil, Restart
	}
	return txn.getObject(vUUId, true), nil
}

func (txn *Txn) getObject(vUUId *common.VarUUId, addToTxn bool) *Object {
	if obj, found := txn.objs[*vUUId]; found {
		return obj
	}

	if txn.parent != nil {
		if obj := txn.parent.getObject(vUUId, false); obj != nil {
			if addToTxn {
				obj.state = obj.state.clone(txn)
				txn.objs[*vUUId] = obj
			}
			return obj
		}
	}

	if addToTxn {
		obj := &Object{
			Id:   vUUId,
			conn: txn.conn,
		}
		txn.objs[*obj.Id] = obj
		obj.state = &objectState{Object: obj, txn: txn}
		return obj
	}

	return nil
}

func (txn *Txn) String() string {
	return fmt.Sprintf("txn_%p(%p)", txn, txn.parent)
}

// Object represents an object in the database. Objects are linked to
// Connections: if you're using multiple Connections, it is not
// permitted to use the same Object in both connections; instead, you
// should retrieve the same Object Id through both
// connections. However, within the same Connection, Objects may be
// reused and pointer equality will work as expected. This is true for
// also nested transactions.
type Object struct {
	ObjectCapabilityPair
	// The unique Id of the object.
	Id    *common.VarUUId
	conn  *Connection
	state *objectState
}

type ObjectCapabilityPair struct {
	*Object
	capability *common.Capability
}

func (ocp ObjectCapabilityPair) String() string {
	return fmt.Sprintf("Reference to %v with %v", ocp.Object.Id, ocp.capability)
}

func (ocp ObjectCapabilityPair) Capability() Capability {
	switch ocp.capability.Which() {
	case msgs.CAPABILITY_NONE:
		return None
	case msgs.CAPABILITY_READ:
		return Read
	case msgs.CAPABILITY_WRITE:
		return Write
	default:
		return ReadWrite
	}
}

type objectState struct {
	*Object
	parentState   *objectState
	txn           *Txn
	curVersion    *common.TxnId
	curValue      []byte
	curObjectRefs []ObjectCapabilityPair
	read          bool
	write         bool
	create        bool
}

func (o *objectState) clone(txn *Txn) *objectState {
	return &objectState{
		Object:        o.Object,
		parentState:   o,
		txn:           txn,
		curVersion:    o.curVersion,
		curValue:      o.curValue,
		curObjectRefs: o.curObjectRefs,
		read:          o.read,
		write:         o.write,
		create:        o.create,
	}
}

func (o *Object) maybeRecordRead(ignoreWritten bool) error {
	state := o.state
	if state.create || state.read || (state.write && !ignoreWritten) {
		return nil
	}
	valueRef := state.txn.cache.Get(o.Id)
	if valueRef == nil {
		modifiedVars, elapsed, err := loadVar(o.Id, o.conn)
		if err != nil {
			return err
		}
		if state.txn.varsUpdated(modifiedVars) {
			return Restart
		}
		valueRef = state.txn.cache.Get(o.Id)
		if valueRef == nil {
			return fmt.Errorf("Loading var %v failed to find value / update cache", o.Id)
		}
		// log.Println(o.state.txn, "load", o.Id, "->", valueRef.version, modifiedVars)
		state.txn.stats.Loads[*o.Id] = elapsed
	}
	state.read = true
	state.curVersion = valueRef.version
	if !state.write {
		state.curValue = valueRef.value
		refs := make([]ObjectCapabilityPair, len(valueRef.references))
		var err error
		for idx, rc := range valueRef.references {
			if rc.vUUId != nil {
				ocp := &refs[idx]
				ocp.capability = rc.capability
				ocp.Object, err = state.txn.GetObject(rc.vUUId)
				if err != nil {
					return err
				}
				ocp.Object.capability = ocp.Object.capability.Union(ocp.capability)
			}
		}
		state.curObjectRefs = refs
	}
	return nil
}

type Capability uint8

const (
	None      Capability = iota
	Read      Capability = iota
	Write     Capability = iota
	ReadWrite Capability = iota
)

func (o *Object) GrantCapability(capability Capability) ObjectCapabilityPair {
	seg := capn.NewBuffer(nil)
	cap := msgs.NewCapability(seg)
	switch capability {
	case None:
		cap.SetNone()
	case Read:
		cap.SetRead()
	case Write:
		cap.SetWrite()
	case ReadWrite:
		cap.SetReadWrite()
	}

	return ObjectCapabilityPair{
		Object:     o,
		capability: common.NewCapability(cap),
	}
}

// Returns the TxnId of the last transaction that wrote to this
// object. If an error is returned, the current transaction should
// immediately be restarted (return the error Restart)
func (o *Object) Version() (*common.TxnId, error) {
	if err := o.checkExpired(); err != nil {
		return nil, err
	}
	if o.state.create {
		return nil, nil
	}
	if err := o.maybeRecordRead(true); err != nil {
		return nil, err
	}
	return o.state.curVersion, nil
}

// Returns the current value of this object. If an error is returned,
// the current transaction should immediately be restarted (return the
// error Restart). Returns a copy of the current value so you are safe
// to modify it but you will need to call the Set method for any
// modifications to take effect.
func (o *Object) Value() ([]byte, error) {
	if err := o.checkExpired(); err != nil {
		return nil, err
	}
	if err := o.maybeRecordRead(false); err != nil {
		return nil, err
	}
	c := make([]byte, len(o.state.curValue))
	copy(c, o.state.curValue)
	return c, nil
}

// Returns the list of Objects to which this object refers. If an
// error is returned, the current transaction should immediately be
// restarted (return the error Restart). Returns a copy of the current
// references so you are safe to modify it, but you will need to call
// the Set method for any modifications to take effect.
func (o *Object) References() ([]ObjectCapabilityPair, error) {
	if err := o.checkExpired(); err != nil {
		return nil, err
	}
	if err := o.maybeRecordRead(false); err != nil {
		return nil, err
	}
	rc := make([]ObjectCapabilityPair, len(o.state.curObjectRefs))
	copy(rc, o.state.curObjectRefs)
	return rc, nil
}

// Returns the current value of this object the list of Objects to
// which this object refers. If an error is returned, the current
// transaction should immediately be restarted (return the error
// Restart). Returns a copy of the current value and a copy of the
// current references so you are safe to modify them but you will need
// to call the Set method for any modifications to take effect.
func (o *Object) ValueReferences() ([]byte, []ObjectCapabilityPair, error) {
	if err := o.checkExpired(); err != nil {
		return nil, nil, err
	}
	if err := o.maybeRecordRead(false); err != nil {
		return nil, nil, err
	}
	vc := make([]byte, len(o.state.curValue))
	copy(vc, o.state.curValue)
	rc := make([]ObjectCapabilityPair, len(o.state.curObjectRefs))
	copy(rc, o.state.curObjectRefs)
	return vc, rc, nil
}

// Sets the value and references of the current object. If the value
// contains any references to other objects, they must be explicitly
// declared as references otherwise on retrieval you will not be able
// to navigate to them. Note that the order of references is
// stable. If an error is returned, the current transaction should
// immediately be restarted (return the error Restart). This method
// takes copies of both the value and the references so if you modify
// either after calling this method, your modifications will not take
// effect.
func (o *Object) Set(value []byte, references ...ObjectCapabilityPair) error {
	if err := o.checkExpired(); err != nil {
		return err
	}
	o.state.write = true
	o.state.curValue = make([]byte, len(value))
	copy(o.state.curValue, value)
	o.state.curObjectRefs = make([]ObjectCapabilityPair, len(references))
	copy(o.state.curObjectRefs, references)
	return nil
}

func (o *Object) checkExpired() error {
	if o.state == nil {
		return fmt.Errorf("Use of expired object: %v", o.Id)
	} else if o.state.txn.resetInProgress {
		return Restart
	}
	return nil
}

func loadVar(vUUId *common.VarUUId, conn *Connection) ([]*common.VarUUId, time.Duration, error) {
	start := time.Now()
	seg := capn.NewBuffer(nil)
	cTxn := msgs.NewClientTxn(seg)
	actions := msgs.NewClientActionList(seg, 1)
	cTxn.SetActions(actions)
	action := actions.At(0)
	action.SetVarId(vUUId[:])
	action.SetRead()
	read := action.Read()
	read.SetVersion(common.VersionZero[:])
	_, modifiedVars, err := conn.submitTransaction(&cTxn)
	return modifiedVars, time.Now().Sub(start), err
}
