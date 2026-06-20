// Copyright (c) HashiCorp, Inc.
// SPDX-License-Identifier: MPL-2.0

package command

import (
	physConsul "github.com/openbao/openbao/physical/consul"
	csr "github.com/openbao/openbao/serviceregistration/consul"
)

// init wires the Consul storage backend and Consul service registration into
// the command package's backend registries.
//
// This lives in a dedicated file (rather than inline in commands.go) so that
// command/commands.go stays byte-identical to upstream OpenBao and never
// produces a merge conflict when syncing from upstream. The Go specification
// guarantees that all package-level variable initializers (physicalBackends,
// serviceRegistrations) run before any init() in the same package, so the maps
// are already non-nil here and a plain index assignment is safe and
// order-independent.
//
// If upstream ever reintroduces its own "consul" entry, this init() runs after
// the var initializers and therefore wins (last writer), which is the desired
// behavior for this fork.
func init() {
	physicalBackends["consul"] = physConsul.NewConsulBackend
	serviceRegistrations["consul"] = csr.NewConsulServiceRegistration
}
