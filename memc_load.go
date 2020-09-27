package main

import (
	//"bufio"
	"fmt"
	//"io"
	//"io/ioutil"
	"log"
	"os"
	"strings"
	"strconv"
	"reflect"
	"math"
	"flag"
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
}


type Opts struct {
    IsTest bool
    LogFile string
    Dry bool
    Pattern string
    Idfa string
    Gaid string
    Adid string
    Dvid string
}

func configLog(opts Opts) {
	if opts.LogFile > "" {
		f, err := os.OpenFile(opts.LogFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		if err != nil {
		    log.Fatalf("error opening file: %v", err)
		}
		defer f.Close()

		log.SetOutput(f)
	}	
}

func main() {

	opts := Opts{}

	flag.BoolVar(&opts.IsTest, "t", false, "test mode")
	flag.StringVar(&opts.LogFile, "l", "", "log file")

	flag.BoolVar(&opts.Dry, "dry", false, "run in debug mode")
	flag.StringVar(&opts.Pattern, "pattern", "/data/appsinstalled/*.tsv.gz", "log path pattern")
	flag.StringVar(&opts.Idfa, "idfa", "127.0.0.1:33013", "idfa server address")
	flag.StringVar(&opts.Gaid, "gaid", "127.0.0.1:33014", "gaid server address")
	flag.StringVar(&opts.Adid, "adid", "127.0.0.1:33015", "adid server address")
	flag.StringVar(&opts.Dvid, "dvid", "127.0.0.1:33016", "dvid server address")

	flag.Parse()

	configLog(opts)

	fmt.Println(prototest())
}