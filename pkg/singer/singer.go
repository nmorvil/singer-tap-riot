package singer

import (
	"encoding/json"
	"fmt"
	"github.com/invopop/jsonschema"
	"io"
	"os"
)

type MessageType string

const (
	RecordMessage MessageType = "RECORD"
	SchemaMessage MessageType = "SCHEMA"
	StateMessage  MessageType = "STATE"
)

type Message interface {
	Type() MessageType
	Write(io.Writer) error
}

type Record struct {
	Stream string      `json:"stream"`
	Data   interface{} `json:"data"`
}

func (r Record) Type() MessageType { return RecordMessage }

func (r Record) Write(w io.Writer) error {
	return json.NewEncoder(w).Encode(map[string]interface{}{
		"type":   string(r.Type()),
		"stream": r.Stream,
		"record": r.Data,
	})
}

type Schema struct {
	Stream             string      `json:"stream"`
	Schema             interface{} `json:"schema"`
	KeyProperties      []string    `json:"key_properties,omitempty"`
	BookmarkProperties []string    `json:"bookmark_properties,omitempty"`
}

func (s Schema) Type() MessageType { return SchemaMessage }

func (s Schema) Write(w io.Writer) error {
	type schema struct {
		Type               string      `json:"type"`
		Stream             string      `json:"stream"`
		Schema             interface{} `json:"schema"`
		KeyProperties      []string    `json:"key_properties,omitempty"`
		BookmarkProperties []string    `json:"bookmark_properties,omitempty"`
	}
	msg := schema{
		Type:               string(s.Type()),
		Stream:             s.Stream,
		Schema:             s.Schema,
		KeyProperties:      s.KeyProperties,
		BookmarkProperties: s.BookmarkProperties,
	}
	return json.NewEncoder(w).Encode(msg)
}

type State struct {
	Value map[string]map[string]string `json:"value"`
}

func (s State) Type() MessageType { return StateMessage }

func (s State) Write(w io.Writer) error {
	msg := map[string]interface{}{
		"type":  string(s.Type()),
		"value": s.Value,
	}
	return json.NewEncoder(w).Encode(msg)
}

type Tap struct {
	output io.Writer
	logger Logger
}

type Logger interface {
	Info(format string, args ...interface{})
	Error(format string, args ...interface{})
}

type StderrLogger struct{}

func (l StderrLogger) Info(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "INFO: "+format+"\n", args...)
}

func (l StderrLogger) Error(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: "+format+"\n", args...)
}

func NewTap() *Tap {
	return &Tap{
		output: os.Stdout,
		logger: StderrLogger{},
	}
}

func NewTapWithWriter(w io.Writer) *Tap {
	return &Tap{
		output: w,
		logger: StderrLogger{},
	}
}

func (t *Tap) SetLogger(l Logger) {
	t.logger = l
}

func (t *Tap) WriteRecord(stream string, record interface{}) error {
	r := Record{
		Stream: stream,
		Data:   record,
	}
	return r.Write(t.output)
}

func (t *Tap) WriteSchema(stream string, schema interface{}, keyProperties []string) error {
	s := Schema{
		Stream:        stream,
		Schema:        schema,
		KeyProperties: keyProperties,
	}
	return s.Write(t.output)
}

func (t *Tap) WriteSchemaFromStream(s Stream) error {
	return t.WriteSchema(s.Stream, s.Schema, s.Metadata[0].Metadata["key-properties"].([]string))
}

func (t *Tap) WriteState(state *State) error {
	return state.Write(t.output)
}

func (t *Tap) WriteCatalog(catalog *Catalog) error {
	return json.NewEncoder(t.output).Encode(catalog)
}

func (t *Tap) Log(format string, args ...interface{}) {
	if t.logger != nil {
		t.logger.Info(format, args...)
	}
}

func (t *Tap) LogError(format string, args ...interface{}) {
	if t.logger != nil {
		t.logger.Error(format, args...)
	}
}

// Legacy Config support - deprecated, use internal/config package instead
type Config map[string]interface{}

func (c Config) GetString(key string) (string, bool) {
	if val, ok := c[key]; ok {
		if str, ok := val.(string); ok {
			return str, true
		}
	}
	return "", false
}

func (c Config) GetInt(key string) (int, bool) {
	if val, ok := c[key]; ok {
		switch v := val.(type) {
		case float64:
			return int(v), true
		case int:
			return v, true
		}
	}
	return 0, false
}

type Catalog struct {
	Streams []Stream `json:"streams"`
}

type Stream struct {
	TapStreamID string             `json:"tap_stream_id"`
	Stream      string             `json:"stream"`
	Schema      *jsonschema.Schema `json:"schema"`
	Metadata    []StreamMetadata   `json:"metadata"`
}

type StreamMetadata struct {
	Breadcrumb []string               `json:"breadcrumb"`
	Metadata   map[string]interface{} `json:"metadata"`
}

func LoadState(path string) (*State, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var state State
	err = json.NewDecoder(file).Decode(&state)
	if err != nil {
		return nil, err
	}
	return &state, nil
}

func LoadCatalog(path string) (*Catalog, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var catalog Catalog
	return &catalog, json.NewDecoder(file).Decode(&catalog)
}

func GetSelectedStreams(catalog *Catalog) []string {
	var selected []string
	for _, stream := range catalog.Streams {
		for _, meta := range stream.Metadata {
			if len(meta.Breadcrumb) == 0 {
				if sel, ok := meta.Metadata["selected"].(bool); ok && sel {
					selected = append(selected, stream.Stream)
				}
			}
		}
	}

	if len(selected) == 0 {
		for _, stream := range catalog.Streams {
			selected = append(selected, stream.Stream)
		}
	}

	return selected
}
