package event

import "encoding/json"

type Event[DM any, M any] struct {
	Headers       map[string]string
	DriverMessage DM
	RawData       map[string]interface{}
	Payload       M
}

func (e *Event[DM, M]) Transform(data []byte) error {
	err := json.Unmarshal(data, &e.Payload)
	if err != nil {
		return err
	}

	return nil
}

type SubData[DM any] struct {
	Headers       map[string]string
	DriverMessage DM
	RawData       map[string]interface{}
}
