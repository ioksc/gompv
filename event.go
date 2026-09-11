package gompv

import "encoding/json"

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

// PropertyFloat safely extracts the value of a numeric property as a float64.
func (e Event) PropertyFloat() (string, float64, bool) {
	name, data, ok := e.PropertyChange()
	if !ok {
		return "", 0, false
	}
	f, valid := toFloat(data)
	return name, f, valid
}

// PropertyBool safely extracts the value of a boolean property.
func (e Event) PropertyBool() (string, bool, bool) {
	name, data, ok := e.PropertyChange()
	if !ok {
		return "", false, false
	}
	b, valid := data.(bool)
	return name, b, valid
}

// PropertyString safely extracts a text property, handling both direct strings and json.RawMessage wrappers.
func (e Event) PropertyString() (string, string, bool) {
	name, data, ok := e.PropertyChange()
	if !ok {
		return "", "", false
	}

	switch v := data.(type) {
	case string:
		return name, v, true
	case json.RawMessage:
		var s string
		if json.Unmarshal(v, &s) == nil {
			return name, s, true
		}
	}
	return name, "", false
}

// toFloat encapsulates the internal conversion of mpv numeric types to float64.
func toFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case map[string]any:
		if data, ok := n["data"]; ok {
			return toFloat(data)
		}
		return 0, false
	default:
	}
	return 0, false
}
