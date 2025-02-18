# daggo

Go DAG scheduler.

This repository provides a Go library for scheduling Directed Acyclic Graphs (DAGs) using a modified Heterogeneous Earliest Finish Time (HEFT) algorithm.  
This implementation assumes homogeneous workers (processors), meaning job execution time is consistent across all processors.

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

// get job outer links
oe := dag.OuterEdges(j1)

// get job inner links
ie := dag.InnerEdges(j4)

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

// define a wait group to avoid termination before completion
wg := sync.WaitGroup()

worker := func(id int){
    defer wg.Done()

    for j := range plan.NextJobForWorker(id) {

        // do some work ...

        fmt.Printl("worker %v completed job %v", workerID, j.ID)
        
        plan.Done(j.ID)
    }
}


// run 2 workers
go worker(0)
go worker(1)

wg.Add(2)
wg.Wait()

```
