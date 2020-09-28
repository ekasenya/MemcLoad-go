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
	"runtime"
	"path/filepath"
	"./appsinstalled"

	"github.com/golang/protobuf/proto"
)


const float64EqualityThreshold = 1e-9

type mapWorkers = map[string]*MemcWorker

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
    isTest bool
    logFile string
    dry bool
    pattern string
    idfa string
    gaid string
    adid string
    dvid string
}

func(opts Opts) String() string {
	return fmt.Sprintf("t: %s, l: %s, dry: %s, pattern: %s, idfa: %s, gaid: %s, adid: %s, dvid: %s", 
		strconv.FormatBool(opts.isTest), opts.logFile, strconv.FormatBool(opts.dry), opts.pattern, opts.idfa, opts.gaid, opts.adid, opts.dvid) 
}

func configLog(opts Opts) {
	if opts.logFile > "" {
		f, err := os.OpenFile(opts.logFile, os.O_RDWR | os.O_CREATE | os.O_APPEND, 0666)
		if err != nil {
		    log.Fatalf("error opening file: %v", err)
		}
		defer f.Close()

		log.SetOutput(f)
	}	
}

func readOptions() Opts {
	opts := Opts{}

	flag.BoolVar(&opts.isTest, "t", false, "test mode")
	flag.StringVar(&opts.logFile, "l", "", "log file")

	flag.BoolVar(&opts.dry, "dry", false, "run in debug mode")
	flag.StringVar(&opts.pattern, "pattern", "./data/appsinstalled/[^.]*.tsv.gz", "log path pattern")
	flag.StringVar(&opts.idfa, "idfa", "127.0.0.1:33013", "idfa server address")
	flag.StringVar(&opts.gaid, "gaid", "127.0.0.1:33014", "gaid server address")
	flag.StringVar(&opts.adid, "adid", "127.0.0.1:33015", "adid server address")
	flag.StringVar(&opts.dvid, "dvid", "127.0.0.1:33016", "dvid server address")

	flag.Parse()

	return opts
}

func process_file(filename string, memcDict mapWorkers) {
	// processed := 0
	// error := 0

	fmt.Printf("Process file: %s\n", filename)
}


type MemcWorker struct {
	ipAddr string
	receiveChan chan int
	quitChan chan int

}

func writeToMemc(worker MemcWorker) {
	for {
	select {
		case <- worker.quitChan:
			return
		case i := <- worker.receiveChan:
			fmt.Println(worker.ipAddr, i)
		}
	}
}

func createWorkers(opts Opts) mapWorkers {	
    memcDict := make(mapWorkers)

    memcDict["idfa"] = &MemcWorker{}
    memcDict["idfa"].ipAddr = opts.idfa
    memcDict["idfa"].receiveChan = make(chan int)
    memcDict["idfa"].quitChan = make(chan int)   

    memcDict["gaid"] = &MemcWorker{}
    memcDict["gaid"].ipAddr = opts.gaid
    memcDict["gaid"].receiveChan = make(chan int)
    memcDict["gaid"].quitChan = make(chan int)

    memcDict["adid"] = &MemcWorker{}
    memcDict["adid"].ipAddr = opts.adid
    memcDict["adid"].receiveChan = make(chan int)
    memcDict["adid"].quitChan = make(chan int)

    memcDict["dvid"] = &MemcWorker{}
    memcDict["dvid"].ipAddr = opts.dvid
    memcDict["dvid"].receiveChan = make(chan int)
    memcDict["dvid"].quitChan = make(chan int)

    return memcDict

}

func startWorkes(memcDict mapWorkers) {
	for _, worker := range memcDict {
    	go writeToMemc(*	worker)
	}
}

func stopWorkes(memcDict mapWorkers) {
	for _, worker := range memcDict {
    	worker.quitChan <- 0
	}
}


func main() {
	opts := readOptions()
	configLog(opts)

	if opts.isTest {
		if !prototest() {
			log.Println("Test FAILED")
		} else {
			log.Println("Test success")
		}	

		os.Exit(0)
	}

	log.Printf("Memc loader started with options: %s\n", opts)
	fmt.Println(runtime.NumCPU())

	memcDict := createWorkers(opts)
  
	files, _ := filepath.Glob(opts.pattern)

	if len(files) > 0 {
		startWorkes(memcDict)

		for _, filename := range files {
			process_file(filename, memcDict)
		}
		stopWorkes(memcDict)
	}

}