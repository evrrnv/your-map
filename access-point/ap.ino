#include <ESP8266WiFi.h>

void setup()
{
  Serial.begin(9600);
  Serial.println();

  Serial.print("Setting soft-AP ... ");
  boolean result = WiFi.softAP("ACCESS_POINT_NAME", "ACCESS_POINT_PASSWORD");
  if(result == true) {
    Serial.println("Ready");
  }
  else {
    Serial.println("Failed!");
  }
}

void loop() {
  Serial.printf("Stations connected = %d\n", WiFi.softAPgetStationNum());
  delay(3000);
}