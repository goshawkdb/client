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

func (c *cache) updateFromTxnCommit(txn *msgs.ClientTxn) {
	DebugLog(c.logger, "debug", "updating from commit")
	actions := txn.Actions()
	c.lock.Lock()
	defer c.lock.Unlock()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		vUUId := common.MakeVarUUId(action.VarId())
		value := action.Value()
		switch value.Which() {
		case msgs.CLIENTACTIONVALUE_CREATE:
			create := value.Create()
			refs := create.References()
			c.updateFromWrite(vUUId, create.Value(), &refs, true)
		case msgs.CLIENTACTIONVALUE_EXISTING:
			modify := value.Existing().Modify()
			if modify.Which() == msgs.CLIENTACTIONVALUEEXISTINGMODIFY_WRITE {
				write := modify.Write()
				refs := write.References()
				c.updateFromWrite(vUUId, write.Value(), &refs, false)
			}
		default:
			panic(fmt.Sprintf("Unexpected value action! %v", value.Which()))
		}
	}
}

func (c *cache) updateFromTxnAbort(actions *msgs.ClientAction_List) []*common.VarUUId {
	DebugLog(c.logger, "debug", "updating from abort")
	modifiedVars := make([]*common.VarUUId, 0, actions.Len())
	c.lock.Lock()
	defer c.lock.Unlock()
	for idx, l := 0, actions.Len(); idx < l; idx++ {
		action := actions.At(idx)
		vUUId := common.MakeVarUUId(action.VarId())
		DebugLog(c.logger, "debug", "abort", "vUUId", vUUId)
		value := action.Value()
		switch value.Which() {
		case msgs.CLIENTACTIONVALUE_MISSING:
			c.updateFromDelete(vUUId)
			modifiedVars = append(modifiedVars, vUUId)
		case msgs.CLIENTACTIONVALUE_EXISTING:
			// We're missing TxnId and TxnId made a write of vUUId (to
			// version TxnId). Though we don't actually have txnId any
			// more...
			modify := value.Existing().Modify()
			if modify.Which() != msgs.CLIENTACTIONVALUEEXISTINGMODIFY_WRITE {
				panic("EXISTING update should have CLIENTACTIONVALUEEXISTINGMODIFY_WRITE")
			}
			write := modify.Write()
			refs := write.References()
			if c.updateFromWrite(vUUId, write.Value(), &refs, false) {
				modifiedVars = append(modifiedVars, vUUId)
			}
		default:
			panic(fmt.Sprint("Received update with illegal value action:", value.Which()))
		}
	}
	DebugLog(c.logger, "debug", "updating from abort...done")
	return modifiedVars
}

func (c *cache) updateFromDelete(vUUId *common.VarUUId) {
	DebugLog(c.logger, "debug", "updateFromDelete", "vUUId", vUUId)
	if vr, found := c.m[*vUUId]; found && vr.references != nil {
		DebugLog(c.logger, "debug", "removed from cache", "vUUId", vUUId)
		// nb. we do not wipe out the capabilities nor the vr itself!
		vr.value = nil
		vr.references = nil
	} else { // either not found, or found but vr.references == nil
		panic(fmt.Sprint("Divergence discovered on deletion of ", vUUId, ": server thinks we had it cached, but we don't!"))
	}
}

func (c *cache) updateFromWrite(vUUId *common.VarUUId, value []byte, refs *msgs.ClientVarIdPos_List, created bool) bool {
	vr, found := c.m[*vUUId]
	updated := found && vr.references != nil
	references := make([]RefCap, refs.Len())
	if !found && created {
		vr = &valueRef{}
		c.m[*vUUId] = vr
	} else if !found {
		panic(fmt.Sprintf("Received update for unknown vUUId: %v", vUUId))
	}
	if created {
		vr.capability = common.ReadWriteCapability
	}
	vr.references = references
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
	DebugLog(c.logger, "debug", "updated", "vUUId", vUUId, "references", references)
	return updated
}
