package client

import (
	"goshawkdb.io/common"
	msgs "goshawkdb.io/common/capnp"
	// "fmt"
	"log"
	"sync"
)

type valueRef struct {
	version    *common.TxnId
	value      []byte
	references []refCap
}

type refCap struct {
	vUUId        *common.VarUUId
	capabilities msgs.Capabilities
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

func (c *cache) updateFromTxnCommit(txn *msgs.ClientTxn, txnId *common.TxnId) {
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
			c.updateFromWrite(txnId, vUUId, write.Value(), &refs)
		case msgs.CLIENTACTION_READWRITE:
			rw := action.Readwrite()
			refs := rw.References()
			c.updateFromWrite(txnId, vUUId, rw.Value(), &refs)
		case msgs.CLIENTACTION_CREATE:
			create := action.Create()
			refs := create.References()
			c.updateFromWrite(txnId, vUUId, create.Value(), &refs)
		case msgs.CLIENTACTION_READ:
			// do nothing
		}
	}
}

func (c *cache) updateFromTxnAbort(updates *msgs.ClientUpdate_List) []*common.VarUUId {
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
			//fmt.Printf("%v@%v ", vUUId, txnId)
			switch action.Which() {
			case msgs.CLIENTACTION_DELETE:
				c.updateFromDelete(vUUId, txnId)
				modifiedVars = append(modifiedVars, vUUId)
			case msgs.CLIENTACTION_WRITE:
				// We're missing TxnId and TxnId made a write of vUUId (to
				// version TxnId).
				write := action.Write()
				refs := write.References()
				if c.updateFromWrite(txnId, vUUId, write.Value(), &refs) {
					modifiedVars = append(modifiedVars, vUUId)
				}
			default:
				log.Fatal("Received update that was neither a read or write action:", action.Which())
			}
		}
	}
	//fmt.Println(".")
	return modifiedVars
}

func (c *cache) updateFromDelete(vUUId *common.VarUUId, txnId *common.TxnId) {
	if vr, found := c.m[*vUUId]; found && vr.version.Compare(txnId) != common.EQ {
		// fmt.Printf("%v removed from cache (req ver: %v; found ver: %v)\n", vUUId, txnId, vr.version)
		delete(c.m, *vUUId)
	} else if found {
		log.Fatal("Divergence discovered on deletion of ", vUUId, ": server thinks we don't have ", txnId, " but we do!")
	} else {
		log.Fatal("Divergence discovered on deletion of ", vUUId, ": server thinks we had it cached, but we don't!")
	}
}

func (c *cache) updateFromWrite(txnId *common.TxnId, vUUId *common.VarUUId, value []byte, refs *msgs.ClientVarIdPos_List) bool {
	vr, found := c.m[*vUUId]
	references := make([]refCap, refs.Len())
	switch {
	case found && vr.version.Compare(txnId) == common.EQ:
		log.Fatal("Divergence discovered on update of ", vUUId, ": server thinks we don't have ", txnId, " but we do!")
		return false
	case found:
		// Must use the new array because there could be txns in
		// progress that still have pointers to the old array.
		vr.references = references
	default:
		vr = &valueRef{references: references}
		c.m[*vUUId] = vr
	}
	// fmt.Printf("%v updated (%v -> %v)\n", vUUId, vr.version, txnId)
	vr.version = txnId
	vr.value = value
	for idz, n := 0, refs.Len(); idz < n; idz++ {
		ref := refs.At(idz)
		if varId := ref.VarId(); len(varId) > 0 {
			rc := &references[idz]
			rc.vUUId = common.MakeVarUUId(varId)
			rc.capabilities = ref.Capabilities()
		}
	}
	return found
}
