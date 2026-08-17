package domain

type AggregateRoot interface {
	ID() string
	Type() string
	Version() int
	IncrementVersion()
	GetUncommittedEvents() []Event
	ClearUncommittedEvents()
	Apply(event Event, isNew bool)
}

type BaseAggregateRoot struct {
	IDStr             string  `json:"id"`
	TypeStr           string  `json:"type"`
	Vers              int     `json:"version"`
	uncommittedEvents []Event
}

func (a *BaseAggregateRoot) ID() string {
	return a.IDStr
}

func (a *BaseAggregateRoot) Type() string {
	return a.TypeStr
}

func (a *BaseAggregateRoot) Version() int {
	return a.Vers
}

func (a *BaseAggregateRoot) IncrementVersion() {
	a.Vers++
}

func (a *BaseAggregateRoot) GetUncommittedEvents() []Event {
	return a.uncommittedEvents
}

func (a *BaseAggregateRoot) ClearUncommittedEvents() {
	a.uncommittedEvents = nil
}

func (a *BaseAggregateRoot) RaiseEvent(event Event) {
	a.uncommittedEvents = append(a.uncommittedEvents, event)
}
