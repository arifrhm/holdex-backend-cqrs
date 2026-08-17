package graphql

// This file will not be regenerated automatically.
//
// It serves as dependency injection for your app, add any dependencies you require
// here.

import (
	"github.com/holdex/epic-fermi/internal/eventstore"
	"github.com/holdex/epic-fermi/internal/query"
)

type Resolver struct {
	QueryService *query.Service
	EventStore   eventstore.EventStore
}
