package models

import (
	"errors"
	"strings"
	"time"
)

type SensorData struct {
	Timestamp int64 `json:"t"`
	Family string `json:"f"`
	Device string `json:"d"`
	Location string `json:"l,omitempty"`
	Sensors map[string]map[string]interface{} `json:"s"`
	GPS GPS `json:"gps,omitempty"`
}

type GPS struct {
	Latitude  float64 `json:"lat,omitempty"`
	Longitude float64 `json:"lon,omitempty"`
	Altitude  float64 `json:"alt,omitempty"`
}

func (d *SensorData) Validate() (err error) {
	d.Family = strings.TrimSpace(strings.ToLower(d.Family))
	d.Device = strings.TrimSpace(strings.ToLower(d.Device))
	d.Location = strings.TrimSpace(strings.ToLower(d.Location))
	if d.Family == "" {
		err = errors.New("family cannot be empty")
	} else if d.Device == "" {
		err = errors.New("device cannot be empty")
	} else if d.Timestamp < 0 {
		err = errors.New("timestamp is not valid")
	}
	if d.Timestamp == 0 {
		d.Timestamp = time.Now().UTC().UnixNano() / int64(time.Millisecond)
	}
	numFingerprints := 0
	for sensorType := range d.Sensors {
		numFingerprints += len(d.Sensors[sensorType])
	}
	if numFingerprints == 0 {
		err = errors.New("sensor data cannot be empty")
	}
	return
}

type FINDFingerprint struct {
	Group           string   `json:"group"`
	Username        string   `json:"username"`
	Location        string   `json:"location"`
	Timestamp       int64    `json:"timestamp"`
	WifiFingerprint []Router `json:"wifi-fingerprint"`
}

type Router struct {
	Mac  string `json:"mac"`
	Rssi int    `json:"rssi"`
}

func (f FINDFingerprint) Convert() (d SensorData) {
	d = SensorData{
		Timestamp: int64(f.Timestamp),
		Family:    f.Group,
		Device:    f.Username,
		Location:  f.Location,
		Sensors:   make(map[string]map[string]interface{}),
	}
	if len(f.WifiFingerprint) > 0 {
		d.Sensors["wifi"] = make(map[string]interface{})
		for _, fingerprint := range f.WifiFingerprint {
			d.Sensors["wifi"][fingerprint.Mac] = float64(fingerprint.Rssi)
		}
	}
	return
}
