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
	outLinks map[string][]link
	// Links going in from a target node
	inLinks map[string][]link
}

// MakeDAG create a new DAG
func MakeDAG() DAG {
	return DAG{
		Jobs:     make(map[string]Job, 0),
		outLinks: make(map[string][]link, 0),
		inLinks:  make(map[string][]link, 0),
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
	dag.outLinks[j.ID] = make([]link, 0)
	dag.inLinks[j.ID] = make([]link, 0)

	return j
}

// Creates a new DAG link with default values.
//
// By default the data exchange time will be set to 0.
func (dag *DAG) Link(from Job, to Job) {
	dag.LinkWithTime(from, to, 0)
}

// Creates a new DAG link with specified values.
//
// dataExchangeTime is the time used to transfer data between workers at the
// end of job `from`.
func (dag *DAG) LinkWithTime(from Job, to Job, dataExchangeTime float32) {
	v, ok := dag.outLinks[from.ID]
	if !ok {
		v = make([]link, 0, 1)
	}
	v = append(v, link{
		JobID:            to.ID,
		DataExchangeTime: dataExchangeTime,
	})
	dag.outLinks[from.ID] = v

	v, ok = dag.inLinks[to.ID]
	if !ok {
		v = make([]link, 0, 1)
	}
	v = append(v, link{
		JobID:            from.ID,
		DataExchangeTime: dataExchangeTime,
	})
	dag.inLinks[to.ID] = v
}

// Roots returns all the DAG roots
func (dag *DAG) Roots() []Job {
	out := make([]Job, 0)
	for id, v := range dag.inLinks {
		if len(v) == 0 {
			out = append(out, dag.Jobs[id])
		}
	}

	return out
}

// Leaves return all the DAG leaves
func (dag *DAG) Leaves() []Job {
	out := make([]Job, 0)
	for id, v := range dag.outLinks {
		if len(v) == 0 {
			out = append(out, dag.Jobs[id])
		}
	}

	return out
}

// Return all the outgoing links to jobID
func (dag *DAG) OutLinks(jobID string) []link {
	return dag.outLinks[jobID]
}

// Return all the incoming links to jobID
func (dag *DAG) InLinks(jobID string) []link {
	return dag.inLinks[jobID]
}
