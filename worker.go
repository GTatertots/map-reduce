package main

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	_ "github.com/mattn/go-sqlite3"
)

type MapTask struct {
	M, R       int    // total number of map and reduce tasks
	N          int    // map task number, 0-based
	SourceHost string // address of host with map input file
}

type ReduceTask struct {
	M, R        int      // total number of map and reduce tasks
	N           int      // reduce task number, 0-based
	SourceHosts []string // addresses of map workers
}

type Pair struct {
	Key   string
	Value string
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

func getLocalAddress() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	localAddr := conn.LocalAddr().(*net.UDPAddr)

	localaddress := localAddr.IP.String()

	if localaddress == "" {
		panic("init: failed to find non-loopback interface with valid address on this node")
	}
	return localaddress
}

type Client struct{}

func (c Client) Map(key, value string, output chan<- Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := Pair{Key: key, Value: strconv.Itoa(count)}
	output <- p
	return nil
}

func (task *MapTask) Process(tempdir string, client Interface) error {
	// logging stuff
	countPairs := 0
	countGenerated := 0
	// Download and open the input file
	if err := download(makeURL(task.SourceHost, mapSourceFile(task.N)), filepath.Join(tempdir, mapInputFile(task.N))); err != nil {
		log.Fatalf("unable to download input file: %v", err)
	}
	db, err := openDatabase(filepath.Join(tempdir, mapInputFile(task.N)))
	if err != nil {
		log.Fatalf("unable to open input file: %v", err)
	}
	defer db.Close()
	var outputDBs []*sql.DB
	// Create the output files
	for i := 0; i < task.R; i++ {
		outputFile, err := createDatabase(filepath.Join(tempdir, mapOutputFile(task.N, i)))
		if err != nil {
			log.Fatalf("failed to create output file: %v", err)
		}
		defer outputFile.Close()
		outputDBs = append(outputDBs, outputFile)
	}

	// Run a database query to select all pairs from the source file.
	//   For each pair:
	//     Call `client.Map` with the data pair
	//     Gather all Pair objects the client feeds back through the
	//     output channel and insert each pair into the appropriate
	//     output database. This process stops when the client closes
	//     the channel.

	rows, err := db.Query("select key, value from pairs")
	if err != nil {
		log.Printf("error in select query from database maptask process: %v", err)
		return err
	}
	defer rows.Close()
	for rows.Next() {
		countPairs++
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			log.Fatalf("error scanning row value maptask process: %v", err)
			return err
		}	
    outputChannel := make(chan Pair)
    finished := make(chan int)
    go func() {
      for pair := range outputChannel {
        countGenerated++
        hash := fnv.New32() // from the stdlib package hash/fnv
        hash.Write([]byte(pair.Key))
        r := int(hash.Sum32() % uint32(task.R))
        db := outputDBs[r]
        if _, err := db.Exec(`insert into pairs (key, value) values (?, ?)`, pair.Key, pair.Value); err != nil {
          log.Printf("db error inserting row to maptask process output database: %v", err)
          //return err
        }
      }
      finished<-0
    }()
		client.Map(key, value, outputChannel)
    <-finished
	}
	if err := rows.Err(); err != nil {
		log.Printf("db error iterating over inputs maptask process: %v", err)
		return err
	}
	log.Printf("map task processed %d pairs, generated %d pairs", countPairs, countGenerated)
	return nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	// Create the input database by merging all of the appropriate
	// output databases from the map phase
	var urls []string
	for i := range task.SourceHosts {
		urls = append(urls, makeURL(task.SourceHosts[i], mapOutputFile(i, task.N)))
	}
	db, err := mergeDatabases(urls, filepath.Join(tempdir,reduceInputFile(task.N)), filepath.Join(tempdir, reduceTempFile(task.N)))
	if err != nil {
		log.Fatalf("merge database error reducetask process: %v", err)
	}
	defer db.Close()

	// Create the output database
	outputDB, err := createDatabase(filepath.Join(tempdir, reduceOutputFile(task.N)))
	if err != nil {
		log.Fatalf("failed to create output database reducetask process: %v", err)
	}
	defer outputDB.Close()

	//
	// Process all pairs in the correct order. This is trickier than in
	// the map phase. Use this query:
	//
	//     select key, value from pairs order by key, value
	//
	// It will sort all of the data and return it in the proper order.
	// As you loop over the key/value pairs, take note whether the key
	// for a new row is the same or different from the key of the
	// previous row.
	//
	// When you encounter a key for the first time:
	//
	//    Close out the previous call to `client.Reduce` (unless this
	//    is the first key, of course). This includes closing the
	//    input channel (so `Reduce` will know it has processed all
	//    values for the given key) and waiting for it to finish.
	//    Start a new call to `client.Reduce`. Carefully plan how you
	//    will manage the necessary goroutines and channels. This
	//    includes receiving output pairs and inserting them into the
	//    output database.
	//
	rows, err := db.Query("select key, value from pairs order by key, value")
	if err != nil {
		log.Printf("error in select query from database reducetask process: %v", err)
		return err
	}
	defer rows.Close()
	prevKey := ""
	outputChannel := make(chan Pair)
	countRows := 0
	values := make(chan string)
	countedKeys := 0
	countedValues := 0
	countedPairs := 0

	for rows.Next() {
		countedValues++
		countRows++
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			log.Fatalf("error scanning row value reducetask process: %v", err)
			return err
		}
		
		//encountering a key for the first time
		if key != prevKey {
			countedPairs++
			countedKeys++
			if countRows != 1 {
				close(values)
				values = make(chan string)
			}
			go func() {
				client.Reduce(key, values, outputChannel)
			}()
		}
		values <- value
		prevKey = key
	}
	if err := rows.Err(); err != nil {
		log.Fatalf("db error iterating over inputs maptask process: %v", err)
		return err
	}
	close(values)

	// Be vigilant about watching for and handling errors in all code. Make
	// sure that you close all databases before returning.
	//
	// Also, when you finish processing all rows, do not forget to close
	// out the final call to `client.Reduce`.
	log.Printf("reduce task processed %d keys and %d values, generated %d pairs\n", countedKeys, countedValues, countedPairs)
	return nil
}

func main() {
	m := 9
	r := 3
	source := "source.db"
	//target := "target.db"
	tmp := os.TempDir()

	tempdir := filepath.Join(tmp, fmt.Sprintf("mapreduce.%d", os.Getpid()))
	if err := os.RemoveAll(tempdir); err != nil {
		log.Fatalf("unable to delete old temp dir: %v", err)
	}
	if err := os.Mkdir(tempdir, 0700); err != nil {
		log.Fatalf("unable to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempdir)

	log.Printf("splitting %s into %d pieces", source, m)
	var paths []string
	for i := 0; i < m; i++ {
		paths = append(paths, filepath.Join(tempdir, mapSourceFile(i)))
	}
	if err := splitDatabase(source, paths); err != nil {
		log.Fatalf("splitting database: %v", err)
	}

	myAddress := net.JoinHostPort(getLocalAddress(), "3410")
	log.Printf("starting http server at %s", myAddress)
	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir(tempdir))))
	listener, err := net.Listen("tcp", myAddress)
	if err != nil {
		log.Fatalf("Error in HTTP server listen for %s: %v", myAddress, err)
	}
	go func() {
		if err := http.Serve(listener, nil); err != nil {
			log.Fatalf("Error in HTTP server serve for %s: %v", myAddress, err)
		}
	}()

	// build the map tasks
	var mapTasks []*MapTask
	for i := 0; i < m; i++ {
		task := &MapTask{
			M:          m,
			R:          r,
			N:          i,
			SourceHost: myAddress,
		}
		mapTasks = append(mapTasks, task)
	}

	// build the reduce tasks
	var reduceTasks []*ReduceTask
	for i := 0; i < r; i++ {
		task := &ReduceTask{
			M:           m,
			R:           r,
			N:           i,
			SourceHosts: make([]string, m),
		}
		reduceTasks = append(reduceTasks, task)
	}

	var client Client

	// process the map tasks
	for i, task := range mapTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("processing map task %d: %v", i, err)
		}
		for _, reduce := range reduceTasks {
			reduce.SourceHosts[i] = myAddress
		}
	}

	// process the reduce tasks
	for i, task := range reduceTasks {
		if err := task.Process(tempdir, client); err != nil {
			log.Fatalf("processing reduce task %d: %v", i, err)
		}
	}
	// gather outputs into final target.db file
  var urls []string
	for i := range reduceTasks {
		urls = append(urls, makeURL(myAddress, reduceOutputFile(i)))
	}
	db, err := mergeDatabases(urls, "target.db", tempdir)
	if err != nil {
		log.Fatalf("merge database error main output: %v", err)
	}
	defer db.Close()
}
