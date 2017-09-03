package client

import (
	"fmt"
	"goshawkdb.io/common"
)

// reference to some other var. Also contains a capability.
type RefCap struct {
	vUUId      *common.VarUUId
	capability *common.Capability
}

func (rc RefCap) String() string {
	return fmt.Sprintf("%v(%v)", rc.vUUId, rc.capability)
}

func (rc RefCap) DenyRead() RefCap {
	return RefCap{
		vUUId:      rc.vUUId,
		capability: rc.capability.DenyRead(),
	}
}

func (rc RefCap) DenyWrite() RefCap {
	return RefCap{
		vUUId:      rc.vUUId,
		capability: rc.capability.DenyWrite(),
	}
}

func (rc RefCap) CanRead() bool {
	return rc.capability.CanRead()
}

func (rc RefCap) CanWrite() bool {
	return rc.capability.CanWrite()
}

func (a RefCap) SameReferent(b RefCap) bool {
	return a.vUUId.Compare(b.vUUId) == common.EQ
}

func (rc RefCap) GrantCapability(cap *common.Capability) RefCap {
	return RefCap{
		vUUId:      rc.vUUId,
		capability: cap,
	}
}

func (rc RefCap) Id() *common.VarUUId {
	return rc.vUUId
}

func (rc RefCap) RefCapability() *common.Capability {
	return rc.capability
}
