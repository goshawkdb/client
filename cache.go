package client

import (
	"fmt"
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
	"log"
	"sync"
)

type valueRef struct {
	version    *common.TxnId
	capability *common.Capability
	value      []byte
	references []refCap
}

type refCap struct {
	vUUId      *common.VarUUId
	capability *common.Capability
}

func (rc refCap) String() string {
	return fmt.Sprintf("%v(%v)", rc.vUUId, rc.capability)
}

type cache struct {
	sync.RWMutex
	m map[common.VarUUId]*valueRef
}

func newCache() *cache {
	return &cache{
		m: make(map[common.VarUUId]*valueRef),
	}
}

func (c *cache) Get(vUUId *common.VarUUId) *valueRef {
	c.RLock()
	defer c.RUnlock()
	return c.m[*vUUId]
}

func (c *cache) SetRoots(roots map[string]*refCap) {
	for _, rc := range roots {
		c.m[*rc.vUUId] = &valueRef{capability: rc.capability}
	}
}

func (c *cache) updateFromTxnCommit(txn *msgs.ClientTxn, txnId *common.TxnId) {
	// fmt.Println("Updating from commit")
	actions := txn.Actions()
	c.Lock()
	defer c.Unlock()
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
		}
	}
}

func (c *cache) updateFromTxnAbort(updates *msgs.ClientUpdate_List) []*common.VarUUId {
	// fmt.Println("Updating from abort")
	modifiedVars := make([]*common.VarUUId, 0, updates.Len())
	c.Lock()
	defer c.Unlock()
	for idx, l := 0, updates.Len(); idx < l; idx++ {
		update := updates.At(idx)
		txnId := common.MakeTxnId(update.Version())
		actions := update.Actions()
		for idy, m := 0, actions.Len(); idy < m; idy++ {
			action := actions.At(idy)
			vUUId := common.MakeVarUUId(action.VarId())
			// fmt.Printf("abort %v@%v ", vUUId, txnId)
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
				log.Fatal("Received update that was neither a read or write action:", action.Which())
			}
		}
	}
	// fmt.Println(".")
	return modifiedVars
}

func (c *cache) updateFromDelete(vUUId *common.VarUUId, txnId *common.TxnId) {
	if vr, found := c.m[*vUUId]; found && vr.version != nil && vr.version.Compare(txnId) != common.EQ {
		// fmt.Printf("%v removed from cache (req ver: %v; found ver: %v)\n", vUUId, txnId, vr.version)
		vr.version = nil
		vr.value = nil
		vr.references = nil
	} else if found {
		log.Fatal("Divergence discovered on deletion of ", vUUId, ": server thinks we don't have ", txnId, " but we do!")
	} else {
		log.Fatal("Divergence discovered on deletion of ", vUUId, ": server thinks we had it cached, but we don't!")
	}
}

func (c *cache) updateFromWrite(txnId *common.TxnId, vUUId *common.VarUUId, value []byte, refs *msgs.ClientVarIdPos_List, created bool) bool {
	vr, found := c.m[*vUUId]
	updated := found && vr.version != nil
	references := make([]refCap, refs.Len())
	switch {
	case found && vr.version.Compare(txnId) == common.EQ:
		log.Fatal("Divergence discovered on update of ", vUUId, ": server thinks we don't have ", txnId, " but we do!")
		return false
	case found:
	default:
		vr = &valueRef{}
		c.m[*vUUId] = vr
	}
	if created {
		vr.capability = common.MaxCapability
	}
	// fmt.Printf("%v updated (%v -> %v)\n", vUUId, vr.version, txnId)
	vr.references = references
	vr.version = txnId
	vr.value = value
	for idz, n := 0, refs.Len(); idz < n; idz++ {
		ref := refs.At(idz)
		if varId := ref.VarId(); len(varId) == common.KeyLen {
			rc := &references[idz]
			rc.vUUId = common.MakeVarUUId(varId)
			rc.capability = common.NewCapability(ref.Capability())
			vr, found := c.m[*rc.vUUId]
			if found {
				vr.capability = vr.capability.Union(rc.capability)
			} else {
				vr = &valueRef{capability: rc.capability}
				c.m[*rc.vUUId] = vr
			}
		}
	}
	// fmt.Printf("%v@%v (%v)\n   (-> %v)\n", vUUId, txnId, value, references)
	return updated
}
