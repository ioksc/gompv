package gompv

import (
	"encoding/json"
	"testing"
)

func TestEventPropertyGetters(t *testing.T) {
	evFloat := Event{
		Type: EventPropertyChange,
		Raw: map[string]any{
			"name": "volume",
			"data": 75.5,
		},
	}
	name, valFloat, ok := evFloat.PropertyFloat()
	if !ok || name != "volume" || valFloat != 75.5 {
		t.Errorf("PropertyFloat() = (%q, %v, %v), want (volume, 75.5, true)", name, valFloat, ok)
	}

	evBool := Event{
		Type: EventPropertyChange,
		Raw: map[string]any{
			"name": "pause",
			"data": true,
		},
	}
	name, valBool, ok := evBool.PropertyBool()
	if !ok || name != "pause" || !valBool {
		t.Errorf("PropertyBool() = (%q, %v, %v), want (pause, true, true)", name, valBool, ok)
	}

	evString := Event{
		Type: EventPropertyChange,
		Raw: map[string]any{
			"name": "media-title",
			"data": "Sample Track",
		},
	}
	name, valStr, ok := evString.PropertyString()
	if !ok || name != "media-title" || valStr != "Sample Track" {
		t.Errorf("PropertyString() = (%q, %v, %v), want (media-title, Sample Track, true)", name, valStr, ok)
	}

	evRawString := Event{
		Type: EventPropertyChange,
		Raw: map[string]any{
			"name": "media-title",
			"data": json.RawMessage(`"Sample Track Raw"`),
		},
	}
	name, valRawStr, ok := evRawString.PropertyString()
	if !ok || name != "media-title" || valRawStr != "Sample Track Raw" {
		t.Errorf("PropertyString() from json.RawMessage = (%q, %v, %v), want (media-title, Sample Track Raw, true)", name, valRawStr, ok)
	}

	evOther := Event{
		Type: EventIdle,
		Raw:  map[string]any{},
	}
	if _, _, ok := evOther.PropertyChange(); ok {
		t.Error("PropertyChange() on EventIdle returned ok = true, want false")
	}
}
