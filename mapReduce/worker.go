package mapreduce

import (
	"fmt"
  "log"
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

// functions to generate suitable names
func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.db", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.db", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.db", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.db", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.db", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.db", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.db", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

// map task
func (task *MapTask) Process(tempdir string, client Interface) error {
  err := download(task.SourceHost, tempdir)
  if err != nil {
    log.Fatalf("worker.go: Process & download - %v", err)
  }
  outputFileNames := make([]string, 0)
  for i := 0; i < 16; i++ {
    outputFileNames = append(outputFileNames, mapInputFile(i))
  }
  err = splitDatabase(tempdir, outputFileNames)
  if err != nil {
    log.Fatalf("worker.go: Process & splitDatabase - %v", err)
  }
  for fileName := range outputFileNames {
    client.Map(fileName,)
  }
	return nil
}
