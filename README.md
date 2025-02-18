daggo
![GitHub License](https://img.shields.io/github/license/dilicait/daggo)
![tests](https://github.com/dilicait/daggo/actions/workflows/tests.yaml/badge.svg)
=====

Go DAG scheduler.

This repository provides a Go library for scheduling Directed Acyclic Graphs (DAGs) using a modified [Heterogeneous Earliest Finish Time](https://en.wikipedia.org/wiki/Heterogeneous_earliest_finish_time) (HEFT) algorithm.  
This implementation assumes homogeneous workers, meaning job execution time is consistent across all processors.

## Install

```bash
go get github.com/dilicait/daggo.git
```
## Usage

### Terminology

*   **Job:** A single unit of work.
*   **Worker:** An entity that executes jobs.
*   **Scheduler:** Determines the order of job execution and assigns jobs to workers.
*   **Plan:** The schedule of jobs to be executed by workers.
*   **DAG (Directed Acyclic Graph):** A directed graph with no cycles. It's used to represent dependency between jobs. 

### Define a DAG 

```go
dag := daggo.MakeDAG()

// create a set of jobs, by default the execution
// time is set to 1, but it's possible to specify other 
// options for each job
j1 := dag.MakeJob(daggo.Job{})
j2 := dag.MakeJob(daggo.Job{})
j3 := dag.MakeJob(daggo.Job{ExecutionTime: 10})
j4 := dag.MakeJob(daggo.Job{ID:"job4"})

// define the DAG connectivity, if not specified the
// link data exchange time is set to 0.
dag.Link(j1, j2)
dag.Link(j2, j3)
dag.LinkFromValues(j1, j4, 10)

// some simple method to perform DAG operations 

// get all DAG roots
roots := dag.Roots()

// get all DAG leaves
leaves := dag.Leaves()

// get all job's outer links 
olinks := dag.OutLinks(j1)

// get job inner links
ilinks := dag.InLinks(j4)

```

### Compute a schedule

```go
opts := daggo.ScheduleOpts{
    Workers : 2,
}

plan, err := daggo.Schedule(dag, opts)
```

### Run concurrent tasks using the scheduled plan

This is a brief example involving two workers. `daggo.Plan` its designed for concurrent access.

```go
wg := sync.WaitGroup()

worker := func(id int){
    defer wg.Done()

    // Ask for the next scheduled job ...
    for j := range plan.NextJobForWorker(id) {

        // ... do some work ...
        time.Sleep(time.Seconds * time.Duration(j.ExecutionTime))

        // ... inform the plan that the job is completed
        plan.Done(j.ID)
    }
}


// run 2 workers
go worker(0)
go worker(1)

wg.Add(2)
wg.Wait()

```

## Next Features

- Add `dag.IsAcyclic()` to check if the provided DAG is valid.