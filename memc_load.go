package main

import (
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"

	"./appsinstalled"
)

const (
	float64EqualityThreshold = 1e-9
	bufSize                  = 3
	maxRetryAttempts         = 5
	normalErrRate            = 0.01
)

type mapWorkers = map[string]*MemcWorker

type AppsInstalled struct {
	dev_type string
	dev_id   string
	lat      float64
	lon      float64
	apps     []uint32
}

func (app *AppsInstalled) String() string {
	return fmt.Sprintf("dev_type: %s dev_id: %s lat: %.2f lon: %.2f apps: %v", app.dev_type, app.dev_id, app.lat, app.lon, app.apps)
}

type Opts struct {
	isTest  bool
	logFile string
	dry     bool
	pattern string
	idfa    string
	gaid    string
	adid    string
	dvid    string
}

func (opts Opts) String() string {
	return fmt.Sprintf("t: %s, l: %s, dry: %s, pattern: %s, idfa: %s, gaid: %s, adid: %s, dvid: %s",
		strconv.FormatBool(opts.isTest), opts.logFile, strconv.FormatBool(opts.dry), opts.pattern, opts.idfa, opts.gaid, opts.adid, opts.dvid)
}

type MemcWorker struct {
	wg          *sync.WaitGroup
	mc          *memcache.Client
	ipAddr      string
	receiveChan chan *AppsInstalled
	quitChan    chan int
	dry         bool
	processed   int
	errors      int
}

func dotRename(path string) {
	head, fn := filepath.Split(path)

	err := os.Rename(path, filepath.Join(head, "."+fn))
	if err != nil {
		log.Fatal("Cannot rename file %s: %s", path, err)
	}
}

func insertMemcache(worker *MemcWorker, appInfo *AppsInstalled) bool {
	key := fmt.Sprintf("%s:%s", appInfo.dev_type, appInfo.dev_id)

	ua := &appsinstalled.UserApps{
		Apps: appInfo.apps,
		Lat:  &appInfo.lat,
		Lon:  &appInfo.lon,
	}

	if worker.dry {
		log.Printf("%s - %s -> %s\n", worker.ipAddr, key, ua)
		return true
	} else {
		if worker.mc == nil {
			worker.mc = memcache.New(worker.ipAddr)
		}

		packed, err := proto.Marshal(ua)
		if err != nil {
			log.Printf("Marshaling error: %s\n", err)
			worker.errors++
			return false
		}

		attempt_num := 0

		for attempt_num < maxRetryAttempts {
			if worker.mc.Set(&memcache.Item{Key: key, Value: packed}) == nil {
				return true
			}
			attempt_num++
			time.Sleep(time.Duration(attempt_num*10) * time.Millisecond)
		}

		log.Printf("Cannot write to memcache %s", worker.ipAddr)
		return false
	}

}

func writeToMemc(worker *MemcWorker) {
	defer worker.wg.Done()

	for appInfo := range worker.receiveChan {
		if insertMemcache(worker, appInfo) {
			worker.processed++
		} else {
			worker.errors++
		}
	}

	return
}

func parseAppInstalled(line string) (app *AppsInstalled, err error) {
	app = nil
	err = nil

	words := strings.Split(line, "\t")
	raw_apps := strings.Split(words[4], ",")

	lat, err := strconv.ParseFloat(words[2], 64)
	if err != nil {
		log.Printf("lon conversation error: %s\n", err)
		return
	}

	lon, err := strconv.ParseFloat(words[3], 64)
	if err != nil {
		log.Printf("lat conversation error: %s\n", err)
		return
	}

	var apps []uint32

	for _, app := range raw_apps {
		num, err := strconv.ParseUint(app, 10, 32)
		if err != nil {
			log.Printf("Not all user apps are digits: `%s` %s", line, app)
		} else {
			apps = append(apps, uint32(num))
		}

	}

	app = &AppsInstalled{
		dev_type: words[0],
		dev_id:   words[1],
		lat:      lat,
		lon:      lon,
		apps:     apps,
	}

	return
}

func processFile(opts Opts, filename string, wg *sync.WaitGroup) {

	var wgWorkers sync.WaitGroup

	processed := 0
	errors := 0

	log.Printf("Process file: %s\n", filename)

	memcDict := createWorkers(opts, &wgWorkers)

	f, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Error opening file %s %s\n", filename, err)
		return
	}
	defer f.Close()

	gz, err := gzip.NewReader(f)
	if err != nil {
		log.Fatalf("Error opening file %s %s\n", filename, err)
		return
	}
	defer gz.Close()

	wgWorkers.Add(len(memcDict))
	startWorkes(memcDict)

	fileScanner := bufio.NewScanner(gz)

	for fileScanner.Scan() {
		appInfo, err := parseAppInstalled(fileScanner.Text())

		if err != nil {
			errors++
		} else {
			memcWorker, exists := memcDict[appInfo.dev_type]

			if !exists {
				log.Printf("%s. Unknown device type: %s\n", filename, appInfo.dev_type)
			} else {
				memcWorker.receiveChan <- appInfo
			}
		}
	}

	stopWorkes(memcDict)
	wgWorkers.Wait()

	for _, worker := range memcDict {
		processed += worker.processed
		errors += worker.errors
	}

	errRate := float64(errors) / float64(processed)
	if errRate < normalErrRate {
		log.Printf("%s. Acceptable error rate (%.2f). Successfull load\n", filename, errRate)
	} else {
		log.Printf("%s. High error rate (%.2f > %.2f). Failed load", filename, errRate, normalErrRate)
	}

	//dotRename(filename)

	wg.Done()

}

func createWorkers(opts Opts, wg *sync.WaitGroup) mapWorkers {
	memcDict := make(mapWorkers)

	memcDict["idfa"] = &MemcWorker{}
	memcDict["idfa"].ipAddr = opts.idfa
	memcDict["idfa"].receiveChan = make(chan *AppsInstalled, bufSize)
	memcDict["idfa"].wg = wg
	memcDict["idfa"].dry = opts.dry

	memcDict["gaid"] = &MemcWorker{}
	memcDict["gaid"].ipAddr = opts.gaid
	memcDict["gaid"].receiveChan = make(chan *AppsInstalled, bufSize)
	memcDict["gaid"].wg = wg
	memcDict["gaid"].dry = opts.dry

	memcDict["adid"] = &MemcWorker{}
	memcDict["adid"].ipAddr = opts.adid
	memcDict["adid"].receiveChan = make(chan *AppsInstalled, bufSize)
	memcDict["adid"].wg = wg
	memcDict["adid"].dry = opts.dry

	memcDict["dvid"] = &MemcWorker{}
	memcDict["dvid"].ipAddr = opts.dvid
	memcDict["dvid"].receiveChan = make(chan *AppsInstalled, bufSize)
	memcDict["dvid"].wg = wg
	memcDict["dvid"].dry = opts.dry

	return memcDict

}

func startWorkes(memcDict mapWorkers) {
	for _, worker := range memcDict {
		worker.processed = 0
		worker.errors = 0
		go writeToMemc(worker)
	}
}

func stopWorkes(memcDict mapWorkers) {
	for _, worker := range memcDict {
		close(worker.receiveChan)
	}
}

func configLog(opts Opts) {
	if opts.logFile > "" {
		f, err := os.OpenFile(opts.logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
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

func floatEqual(a, b float64) bool {
	return math.Abs(a-b) <= float64EqualityThreshold
}

func prototest() bool {
	sample := "idfa\t1rfw452y52g2gq4g\t55.55\t42.42\t1423,43,567,3,7,23\ngaid\t7rfw452y52g2gq4g\t55.55\t42.42\t7423,424"

	lines := strings.Split(sample, "\n")
	for _, line := range lines {

		appInfo, err := parseAppInstalled(line)
		if err != nil {
			return false
		}

		ua := &appsinstalled.UserApps{
			Apps: appInfo.apps,
			Lat:  &appInfo.lat,
			Lon:  &appInfo.lon,
		}

		data, err := proto.Marshal(ua)
		if err != nil {
			log.Printf("marshaling error: %s\n", err)
			return false
		}

		ua2 := new(appsinstalled.UserApps)
		err = proto.Unmarshal(data, ua2)
		if err != nil {
			log.Printf("umarshaling error: %s\n", err)
			return false
		}

		if !(floatEqual(*ua.Lat, *ua2.Lat) && floatEqual(*ua.Lon, *ua2.Lon) && reflect.DeepEqual(ua.Apps, ua2.Apps)) {
			return false
		}

	}

	return true
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

	files, _ := filepath.Glob(opts.pattern)

	if len(files) > 0 {

		var wg sync.WaitGroup

		for _, filename := range files {
			wg.Add(1)
			go processFile(opts, filename, &wg)
		}

		wg.Wait()
	}

}
