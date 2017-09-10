package client

import (
	"fmt"
	"github.com/go-kit/kit/log"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
	"sync"
)

// Value with references. Holds the union of all capabilites received
// for the value.
type valueRef struct {
	version    *common.TxnId
	capability common.Capability
	value      []byte
	references []RefCap
}

type cache struct {
	m      map[common.VarUUId]*valueRef
	lock   sync.RWMutex
	logger log.Logger
}

func newCache(roots map[string]*RefCap, logger log.Logger) *cache {
	m := make(map[common.VarUUId]*valueRef)
	for _, rc := range roots {
		m[*rc.vUUId] = &valueRef{capability: rc.capability}
	}
	return &cache{
		m:      m,
		logger: logger,
	}
}

func (c *cache) Get(vUUId *common.VarUUId) *valueRef {
	c.lock.RLock()
	defer c.lock.RUnlock()
	return c.m[*vUUId]
}

func (c *cache) updateFromTxnCommit(txn *msgs.ClientTxn, txnId *common.TxnId) {
	DebugLog(c.logger, "debug", "updating from commit")
	actions := txn.Actions()
	c.lock.Lock()
	defer c.lock.Unlock()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		vUUId := common.MakeVarUUId(action.VarId())
		switch action.Which() {
		case msgs.CLIENTACTION_WRITE:
			write := action.Write()
			refs := write.References()
			c.updateFromWrite(txnId, vUUId, write.Value(), &refs, false)
		case msgs.CLIENTACTION_READWRITE:
			rw := action.Readwrite()
			refs := rw.References()
			c.updateFromWrite(txnId, vUUId, rw.Value(), &refs, false)
		case msgs.CLIENTACTION_CREATE:
			create := action.Create()
			refs := create.References()
			c.updateFromWrite(txnId, vUUId, create.Value(), &refs, true)
		case msgs.CLIENTACTION_READ:
			// do nothing
		default:
			panic(fmt.Sprintf("Unexpected action! %v", action.Which()))
		}
	}
}

func (c *cache) updateFromTxnAbort(updates *msgs.ClientUpdate_List) []*common.VarUUId {
	DebugLog(c.logger, "debug", "updating from abort")
	modifiedVars := make([]*common.VarUUId, 0, updates.Len())
	c.lock.Lock()
	defer c.lock.Unlock()
	for idx, l := 0, updates.Len(); idx < l; idx++ {
		update := updates.At(idx)
		txnId := common.MakeTxnId(update.Version())
		actions := update.Actions()
		for idy, m := 0, actions.Len(); idy < m; idy++ {
			action := actions.At(idy)
			vUUId := common.MakeVarUUId(action.VarId())
			DebugLog(c.logger, "debug", "abort", "vUUId", vUUId, "txnId", txnId)
			switch action.Which() {
			case msgs.CLIENTACTION_DELETE:
				c.updateFromDelete(vUUId, txnId)
				modifiedVars = append(modifiedVars, vUUId)
			case msgs.CLIENTACTION_WRITE:
				// We're missing TxnId and TxnId made a write of vUUId (to
				// version TxnId).
				write := action.Write()
				refs := write.References()
				if c.updateFromWrite(txnId, vUUId, write.Value(), &refs, false) {
					modifiedVars = append(modifiedVars, vUUId)
				}
			default:
				panic(fmt.Sprint("Received update that was neither a read or write action:", action.Which()))
			}
		}
	}
	DebugLog(c.logger, "debug", "updating from abort...done")
	return modifiedVars
}

func (c *cache) updateFromDelete(vUUId *common.VarUUId, txnId *common.TxnId) {
	DebugLog(c.logger, "debug", "updateFromDelete", "vUUId", vUUId, "txnId", txnId)
	if vr, found := c.m[*vUUId]; found && vr.version != nil && vr.version.Compare(txnId) != common.EQ {
		DebugLog(c.logger, "debug", "removed from cache", "vUUId", vUUId, "required", txnId, "existing", vr.version)
		// nb. we do not wipe out the capabilities nor the vr itself!
		vr.version = nil
		vr.value = nil
		vr.references = nil
	} else if found && vr.version != nil { // so vr.version == txnId
		panic(fmt.Sprint("Divergence discovered on deletion of ", vUUId, ": server thinks we don't have ", txnId, " but we do!"))
	} else { // either not found, or found but vr.version == nil
		panic(fmt.Sprint("Divergence discovered on deletion of ", vUUId, ": server thinks we had it cached, but we don't!"))
	}
}

func (c *cache) updateFromWrite(txnId *common.TxnId, vUUId *common.VarUUId, value []byte, refs *msgs.ClientVarIdPos_List, created bool) bool {
	vr, found := c.m[*vUUId]
	updated := found && vr.references != nil
	references := make([]RefCap, refs.Len())
	switch {
	case updated && vr.version.Compare(txnId) == common.EQ:
		panic(fmt.Sprint("Divergence discovered on update of ", vUUId, ": server thinks we don't have ", txnId, " but we do!"))
	case found:
	default:
		vr = &valueRef{}
		c.m[*vUUId] = vr
	}
	if created {
		vr.capability = common.ReadWriteCapability
	}
	existing := vr.version
	vr.references = references
	vr.version = txnId
	vr.value = value
	for idz, n := 0, refs.Len(); idz < n; idz++ {
		ref := refs.At(idz)
		rc := &references[idz]
		rc.vUUId = common.MakeVarUUId(ref.VarId())
		rc.capability = common.NewCapability(ref.Capability())

		vr, found := c.m[*rc.vUUId]
		if found {
			vr.capability = vr.capability.Union(rc.capability)
		} else {
			vr = &valueRef{capability: rc.capability}
			c.m[*rc.vUUId] = vr
		}
	}
	DebugLog(c.logger, "debug", "updated", "vUUId", vUUId, "existing", existing, "new", txnId, "references", references)
	return updated
}
