package gompv

// EventType represents the type of event received from mpv.
type EventType string

const (
	EventPropertyChange EventType = "property-change"
	EventEndFile        EventType = "end-file"
	EventStartFile      EventType = "start-file"
	EventIdle           EventType = "idle"
)

// Event encapsulates an mpv event with helper methods to avoid manual type assertions.
type Event struct {
	Type string
	Raw  map[string]any
}

// PropertyChange extracts the property name and its value if the event is a property-change.
func (e Event) PropertyChange() (string, any, bool) {
	if e.Type != string(EventPropertyChange) {
		return "", nil, false
	}
	name, _ := e.Raw["name"].(string)
	return name, e.Raw["data"], true
}
