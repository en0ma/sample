package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"sync"
)

const workersCount  = 3

type pool struct {
	sync.RWMutex
	jobs map[int]*job
	workers []*worker
}

type job struct {
	key int
	url string
}

type worker struct{
	id int
}

type image struct {
	Urls []string `json:"urls"`
}

func main() {
	imageFilePath, err := readFilePathArgs()
	if err != nil {
		log.Fatalln(err.Error())
	}

	image, err := readImageFile(imageFilePath)
	if err != nil {
		log.Fatalln(err.Error())
	}

	workerPool := createWorkerPool(workersCount)
	workerPool.setJobsFromUrls(image)
	workerPool.start()
}

// createWorkerPool creates a pool of workers
func createWorkerPool(workersCount int) *pool {
	workers := make([]*worker, workersCount)
	for i := range workers {
		workers[i] = &worker{id: i}
	}
	return &pool{workers: workers}
}

// setJobsFromUrls builds a job object from image urls and sets the job to the pool
func (p *pool) setJobsFromUrls(img *image) {
	jobs := make(map[int]*job, len(img.Urls))
	for key, url := range img.Urls {
		jobs[key] = &job{url: url, key:key}
	}
	p.jobs = jobs
}

// start will async run each workers and wait until all jobs are processed by the workers
func (p *pool) start() {
	wg := &sync.WaitGroup{}
	wg.Add(len(p.workers)) //wait for n workers
	for _, worker := range p.workers {
		go worker.run(wg, p)
	}
	wg.Wait()
}

//getJob returns a job or nil if there are no jobs
func (p *pool) getJob() *job {
	p.Lock()
	defer p.Unlock()

	for key, job := range p.jobs {
		//naive approach to remove job - so other jobs won't pick it up
		delete(p.jobs, key)
		return job
	}
	return nil
}

// run executes the workers - the workers will keep running to process jobs and exits when there are no more jobs
func (w *worker) run(wg *sync.WaitGroup, p *pool) {
	for {
		job := p.getJob()
		if job == nil {
			break // if there are no more jobs, stop worker
		}
		w.downloadImage(job)
	}
	wg.Done()
}

func (w *worker) downloadImage(j *job) {
	fmt.Println(fmt.Sprintf("worker #%d - Downloading job #%d - %s", w.id, j.key, j.url))

	res, e := http.Get(j.url)
	if e != nil {
		log.Fatal(e)
	}
	defer res.Body.Close()

	file, err := os.Create(fmt.Sprintf(".data/%d.jpg", j.key))
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	_, err = io.Copy(file, res.Body)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(fmt.Sprintf("worker #%d - Completed job #%d - %s", w.id, j.key, j.url))
}

// readImageFile builds an image struct with the image urls
func readImageFile(imageFilePath string) (*image, error) {
	jsonFIle, err := ioutil.ReadFile(imageFilePath)
	if err != nil {
		return nil, err
	}

	content := &image{}
	err = json.Unmarshal(jsonFIle, content)
	if err != nil {
		return nil, err
	}
	return content, nil
}

// readFilePathArgs reads the os args to get the images file path args
func readFilePathArgs() (string, error) {
	args := os.Args
	if len(args) < 2 {
		return "", errors.New("please supply the images.jon file path")
	}
	return args[1], nil
}