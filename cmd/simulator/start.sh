#!/bin/bash

#go run simulator.go --local_path=../.. --output=mllp -mllp_destination 192.168.1.61:5000
go run simulator.go --local_path=../.. --output=mllp -mllp_destination tntreports.northeurope.cloudapp.azure.com:5000
