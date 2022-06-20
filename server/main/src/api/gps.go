package api

import (
	"github.com/evrrnv/your-map/server/main/src/database"
	"github.com/evrrnv/your-map/server/main/src/models"
	"github.com/pkg/errors"
)

func GetGPSData(family string) (gpsData map[string]models.SensorData, err error) {
	gpsData = make(map[string]models.SensorData)

	d, err := database.Open(family, true)
	if err != nil {
		err = errors.Wrap(err, "You need to add learning data first")
		return
	}
	defer d.Close()

	locations, err := d.GetLocations()
	if err != nil {
		err = errors.Wrap(err, "problem getting locations")
		return
	}

	for _, location := range locations {
		gpsData[location] = models.SensorData{
			GPS: models.GPS{
				Latitude:  -1,
				Longitude: -1,
			},
		}
	}

	var autoGPS map[string]models.SensorData
	errGet := d.Get("autoGPS", &autoGPS)
	if errGet == nil {
		for location := range autoGPS {
			gpsData[location] = models.SensorData{
				GPS: models.GPS{
					Latitude:  autoGPS[location].GPS.Latitude,
					Longitude: autoGPS[location].GPS.Longitude,
				},
			}
		}
	}

	var customGPS map[string]models.SensorData
	errGet = d.Get("customGPS", &customGPS)
	if errGet == nil {
		for location := range customGPS {
			gpsData[location] = models.SensorData{
				GPS: models.GPS{
					Latitude:  customGPS[location].GPS.Latitude,
					Longitude: customGPS[location].GPS.Longitude,
				},
			}
		}
	}

	return
}
