// schedule implements a simple HEFT scheduler for assign jobs to heterogeneous workers.
//
// This implementation is based on haip python implementation https://github.com/mrocklin/heft
package dago

import (
	"strconv"

	"github.com/Cabba/claim"
)

// Job represent a batch of work.
type Job struct {
	// Task unique ID
	ID string
	// The average execution time of a task
	ExecutionTime float32
}

type link struct {
	// Target job for the link
	JobID string
	// Time used to echange data between workers
	DataExchangeTime float32
}

type DAG struct {
	// Jobs in the dag
	Jobs map[string]Job

	// Links going out from a target node
	outerLinks map[string][]link
	// Links going in from a target node
	innerLinks map[string][]link
}

// MakeDAG create a new DAG
func MakeDAG() DAG {
	return DAG{
		Jobs:       make(map[string]Job, 0),
		outerLinks: make(map[string][]link, 0),
		innerLinks: make(map[string][]link, 0),
	}
}

// MakeJob creates a new job.
//
// If j is thee empty Job struct by default the job will have an incremental id and
// an execution time of 1.
// No job can have an execution time <= 0, if negative execution time is provided the application
// will panic.
func (dag *DAG) MakeJob(j Job) Job {
	claim.Pre(j.ExecutionTime >= 0)

	if j.ID == "" {
		j.ID = strconv.Itoa(len(dag.Jobs))
	}

	if j.ExecutionTime == 0 {
		j.ExecutionTime = 1
	}

	dag.Jobs[j.ID] = j
	dag.outerLinks[j.ID] = make([]link, 0)
	dag.innerLinks[j.ID] = make([]link, 0)

	return j
}

// Creates a new DAG link with default values.
//
// By default the data exchange time will be set to 0.
func (dag *DAG) Link(from Job, to Job) {
	dag.LinkFromValues(from, to, 0)
}

// Creates a new DAG link with specified values.
//
// dataExchangeTime is the time used to transfer data between workers at the
// end of job `from`.
func (dag *DAG) LinkFromValues(from Job, to Job, dataExchangeTime float32) {
	v, ok := dag.outerLinks[from.ID]
	if !ok {
		v = make([]link, 0, 1)
	}
	v = append(v, link{
		JobID:            to.ID,
		DataExchangeTime: dataExchangeTime,
	})
	dag.outerLinks[from.ID] = v

	v, ok = dag.innerLinks[to.ID]
	if !ok {
		v = make([]link, 0, 1)
	}
	v = append(v, link{
		JobID:            from.ID,
		DataExchangeTime: dataExchangeTime,
	})
	dag.innerLinks[to.ID] = v
}

// Roots returns all the DAG roots
func (dag *DAG) Roots() []Job {
	out := make([]Job, 0)
	for id, v := range dag.innerLinks {
		if len(v) == 0 {
			out = append(out, dag.Jobs[id])
		}
	}

	return out
}

// Leaves return all the DAG leaves
func (dag *DAG) Leaves() []Job {
	out := make([]Job, 0)
	for id, v := range dag.outerLinks {
		if len(v) == 0 {
			out = append(out, dag.Jobs[id])
		}
	}

	return out
}

// Return all the successors jobs in the graph
func (dag *DAG) Next(jobID string) []link {
	return dag.outerLinks[jobID]
}

// Return all the preceding jobs in the graph
func (dag *DAG) Prev(jobID string) []link {
	return dag.innerLinks[jobID]
}
