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

// PropertyFloat extrae de forma segura el valor de una propiedad numérica como float64.
func (e Event) PropertyFloat() (string, float64, bool) {
	name, data, ok := e.PropertyChange()
	if !ok {
		return "", 0, false
	}
	f, valid := toFloat(data)
	return name, f, valid
}

// PropertyBool extrae de forma segura el valor de una propiedad booleana.
func (e Event) PropertyBool() (string, bool, bool) {
	name, data, ok := e.PropertyChange()
	if !ok {
		return "", false, false
	}
	b, valid := data.(bool)
	return name, b, valid
}

// toFloat encapsula la conversión interna de tipos numéricos de mpv a float64.
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
		return 0, false
	}
}
