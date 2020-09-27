package main

import (
	//"bufio"
	"fmt"
	//"io"
	//"io/ioutil"
	//"log"
	//"os"
	"strings"
	"strconv"
	"reflect"
	"math"
	"./appsinstalled"

	"github.com/golang/protobuf/proto"
)


const float64EqualityThreshold = 1e-9

func floatEqual(a, b float64) bool {
    return math.Abs(a - b) <= float64EqualityThreshold
}

func prototest() bool {    
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"


	lines := strings.Split(sample,"\n")
	for _, line := range lines {

		words := strings.Split(line,"\t")
		raw_apps := strings.Split(words[4],",")

		lat, err := strconv.ParseFloat(words[2], 64) 
		if err != nil {
			fmt.Println("lon conversation error: ", err)
			return false
		}

		lon, err := strconv.ParseFloat(words[3], 64)   
		if err != nil {
			fmt.Println("lat conversation error: ", err)
			return false
		}

		var apps []uint32

		for _, app := range raw_apps {
			num, err := strconv.ParseUint(app, 10, 32)
			if err == nil {
				apps = append(apps, uint32(num))	
			}

		}

		ua := &appsinstalled.UserApps{
			Apps:  apps,
			Lat: &lat,
			Lon: &lon,
		}

		data, err := proto.Marshal(ua)
		if err != nil {
			fmt.Println("marshaling error: ", err)
			return false
		}

		ua2 := new(appsinstalled.UserApps)
		err = proto.Unmarshal(data, ua2)
		if err != nil {
			fmt.Println("umarshaling error: ", err)
			return false
		}

		if !(floatEqual(*ua.Lat, *ua2.Lat) && floatEqual(*ua.Lon, *ua2.Lon) && reflect.DeepEqual(ua.Apps, ua2.Apps)) {
			return false
		}

	}	

	return true	


func main() {
	fmt.Println(prototest())
}