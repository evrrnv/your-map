# YourMap
Indoor Localisation System based on WiFi using ESP8266 sensors

The goal of this project was to create a system that capture RSSI of WiFi access points and use it to estimate the postion of a device in indoor location.

# Architectue

![arch-1](https://user-images.githubusercontent.com/75393305/177975899-54a9e31c-289b-455e-afed-65d6c407b5d0.png)

![arch-2](https://user-images.githubusercontent.com/75393305/177976001-4ca27ee9-6ed8-46c5-9383-dced5ff9463c.png)


# Installation 
## Software Requirements
This project was built with the following software:

Golang v1.18.3

Python v3.8.10

Sqlite v3.31.1

## Setup
### Web Server
1. clone the project

```
$ git clone https://github.com/evrrnv/yourmap.git
```

2. Switch to server directory, you’ll two find two directories, the first one is the web server and the second one is ML server

```
$ cd your-map/server
```

3. Build the main server, after the build is done, an executable file named main will be generated

```
$ cd main
$ go build
```

4. Now switch to the ai directory where ML server is located and install all required python dependecies

```
$ cd ../ai
$ pip install -r requirements.txt
```

### Scanner
1. clone scanner

```
https://github.com/evrrnv/yourmap-cli-scanner.git
```

2. Switch to cli-scanner directory and compile the script, an executable file named cli-scanner will be generated

```
$ cd yourmap-cli-scanner
$ go build
```

### Config Access Points
1. The file named ap.ino in the "access-point" directory contains the code for configuring an ESP8266 access point, you just need to set the SSID and password of your access point by updating the fields ACCESS_POINT_NAME and ACCESS_POINT_PASSWORD

## Run

### Web Server

#### Main Server

```
$ ./main
```

#### ML Server

```
$ make production
```

### Scanner

#### Training

```
$ sudo ./cli-scanner -device <DEVICE_NAME> -location <LOCATION_NAME> -sublocation <SUBLOCATION_NAME> -wifi -forever -server <SERVER_URL> -i <NETWORK_INTERFACE_NAME>
```

#### Tracking

```
$ sudo ./cli-scanner -device <DEVICE_NAME> -location <LOCATION_NAME> -wifi -forever -server <SERVER_URL> -i <NETWORK_INTERFACE_NAME>
```
