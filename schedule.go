package dago

import (
	"errors"
	"math"
	"sort"
	"sync"

	"github.com/Cabba/claim"
)

var (
	ErrMalformedDAG = errors.New("malformed DAG")
)

type Plan struct {
	// DAG used to produce the schedule plan
	dag DAG

	// Schedule of job for each worker
	schedule schedule

	// cCurrently scheduled jobs for each workers
	scheduled []chan Job

	// Number of workers used to generate the plan
	workers int

	// Mutex used to protect access to Plan.schedule slice when calling Plan.Done
	mtx sync.Mutex
}

func (p *Plan) NextJobForWorker(worker int) chan Job {
	claim.Pre(worker >= 0)
	claim.Pre(worker < p.workers)
	claim.Pre(p.scheduled != nil)
	claim.Pre(len(p.scheduled) == p.workers)

	// scheduled[worker] is a channel and does not require any mutex lock.
	// scheduled[] is created at plan initialization and never modified since.
	return p.scheduled[worker]
}

// Inform the plan that a job has completed its exectution.
//
// This function is designed to be called by multiple goroutines concurrently.
func (p *Plan) Done(jobID string) {
	claim.Pre(len(jobID) > 0)

	links := p.dag.Next(jobID)

	// This mutex is required since p.schedule
	// can be accessed concurrenly by several goroutines.
	p.mtx.Lock()
	defer p.mtx.Unlock()

	// For each link check if there is a worker with a job in the schedule available.
	for _, link := range links {
		for wi, ws := range p.schedule {
			if len(ws) > 0 && link.JobID == ws[0].ID {
				// Add the job to the worker scheduled set and remove the job from the queue
				p.scheduled[wi] <- p.dag.Jobs[link.JobID]
				// Pop the scheduled job
				p.schedule[wi] = p.schedule[wi][1:]
				break
			}
		}
	}

	// Check for each worker if there are job to schedule, if not close the
	// scheduled channel.
	// The channel is set to nil to avoid double channel close
	for wi, ws := range p.schedule {
		if len(ws) == 0 && p.scheduled[wi] != nil {
			close(p.scheduled[wi])
			p.scheduled[wi] = nil
		}
	}
}

type ScheduleOpts struct {
	// Number of workers to be used for schedule plan generation
	Workers int
}

// Schedule a plan for a given DAG using the passed options.
//
// To specify ther number of workers.
func Schedule(dag DAG, opts ScheduleOpts) (*Plan, error) {
	claim.Pre(len(dag.Jobs) > 0)
	claim.Pre(opts.Workers > 0)

	jn := len(dag.Jobs)

	jobs := ranku(dag, opts)

	plan := Plan{
		scheduled: make([]chan Job, opts.Workers),
		dag:       dag,
		workers:   opts.Workers,
		schedule:  allocate(dag, jobs, opts),
	}

	// create a channel for each worker
	for i := range opts.Workers {
		// set maximum channel capacity to job number
		plan.scheduled[i] = make(chan Job, jn)
	}

	// push all jobs with start time == 0 in the scheduled queue
	// and pop it from the schedule list
	for wi, ws := range plan.schedule {
		if len(ws) > 0 && ws[0].StartTime == 0 {
			plan.scheduled[wi] <- ws[0].Job
			plan.schedule[wi] = plan.schedule[wi][1:]
		}
	}

	return &plan, nil
}

var InvalidRank float32 = -1

// eXtended job structure, used mainly to sort ranked jobs
type xjob struct {
	Job
	// Rank (upward) value
	Rank float32

	StartTime float32
	EndTime   float32
	Worker    int
}

type schedule [][]xjob

// Find a job in the schedule
func (s *schedule) Job(jobID string) xjob {
	for i := range *s {
		for j := range (*s)[i] {
			if jobID == (*s)[i][j].ID {
				return (*s)[i][j]
			}
		}
	}

	return xjob{}
}

// Computes start and end time for a given job on a specified worker.
//
// This function assumes that s (schedule) is sorted by job start time (lower to higher).
func scheduleTimes(dag DAG, job xjob, worker int, s schedule) (start, end float32) {

	links := dag.Prev(job.ID)

	// compute readyTime for this worker
	readyTime := float32(0)
	for _, link := range links {
		j := s.Job(link.JobID)
		v := float32(0)
		if j.Worker == worker {
			v = j.EndTime
		} else {
			// for other workers add data exchange time
			v = j.EndTime + link.DataExchangeTime
		}
		if readyTime < v {
			readyTime = v
		}
	}

	// check if there is an available scheduling gap after readyTime
	for i := 0; i < len(s[worker])-1; i++ {
		st := s[worker][i+1].StartTime
		et := s[worker][i].EndTime
		if st > readyTime && st-et > job.ExecutionTime {
			return st, st + job.ExecutionTime
		}
	}

	lwi := len(s[worker]) - 1 // last worker index
	st := readyTime           // start time

	if lwi >= 0 && s[worker][lwi].EndTime > readyTime {
		st = s[worker][lwi].EndTime
	}

	return st, st + job.ExecutionTime
}

// Allocate jobs on workers.
func allocate(dag DAG, jobs []xjob, opts ScheduleOpts) schedule {

	sched := make(schedule, opts.Workers)
	for i := range opts.Workers {
		sched[i] = make([]xjob, 0)
	}

	for _, job := range jobs {
		job.EndTime = float32(math.MaxFloat32)

		for worker := range opts.Workers {
			st, et := scheduleTimes(dag, job, worker, sched)
			if job.EndTime > et {
				job.StartTime = st
				job.EndTime = et
				job.Worker = worker
			}
		}

		sched[job.Worker] = append(sched[job.Worker], job)

		// sort by start time, lower to higher
		s := sched[job.Worker]
		sort.Slice(s, func(i, j int) bool {
			return s[i].StartTime < s[j].StartTime
		})
	}

	return sched
}

// ranku refturns a slice of ranked jobs, from job with higher ranking to lower
func ranku(dag DAG, opts ScheduleOpts) []xjob {
	claim.Pre(len(dag.Jobs) > 0)

	jn := len(dag.Jobs)
	out := make([]xjob, jn)

	// compute rank for each job
	c := 0
	for _, v := range dag.Jobs {
		out[c] = xjob{
			Job:  v,
			Rank: rankuval(dag, v, opts),
		}
		c++
	}

	// sort jobs by decreasing order of rank
	sort.Slice(out, func(i, j int) bool {
		return out[i].Rank > out[j].Rank
	})

	return out
}

// rankuval computes the rank value for a given node recursively
func rankuval(dag DAG, j Job, opts ScheduleOpts) float32 {
	links := dag.Next(j.ID)
	if len(links) == 0 {
		return j.ExecutionTime
	}

	max := InvalidRank
	for _, l := range links {
		v := rankuval(dag, dag.Jobs[l.JobID], opts) + l.DataExchangeTime
		if max < v {
			max = v
		}
	}

	return max + j.ExecutionTime
}
