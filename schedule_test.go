package dago

import (
	"sync"
	"testing"
	"time"
)

func TestRanku(t *testing.T) {
	dag := MakeDAG()

	j1 := dag.MakeJob(Job{})
	j2 := dag.MakeJob(Job{})
	j3 := dag.MakeJob(Job{})
	j4 := dag.MakeJob(Job{})
	j5 := dag.MakeJob(Job{})

	dag.LinkFromValues(j1, j2, 10)
	dag.LinkFromValues(j2, j3, 20)
	dag.LinkFromValues(j1, j4, 30)
	dag.LinkFromValues(j4, j5, 10)
	dag.LinkFromValues(j3, j5, 10)

	opts := ScheduleOpts{}

	jobs := ranku(dag, opts)

	if len(jobs) != len(dag.Jobs) {
		t.Errorf("ranku output job number mismatch, got %v expected %v", len(jobs), len(dag.Jobs))
	}

	for i := range jobs {
		if i+1 < len(jobs) && jobs[i].Rank < jobs[i+1].Rank {
			t.Error("ranking is not monotone decreasing")
		}
	}

}

func TestScheduleSingleWorker(t *testing.T) {
	dag := MakeDAG()

	j1 := dag.MakeJob(Job{ExecutionTime: 10})
	j2 := dag.MakeJob(Job{ExecutionTime: 10})
	j3 := dag.MakeJob(Job{ExecutionTime: 10})
	j4 := dag.MakeJob(Job{ExecutionTime: 10})
	j5 := dag.MakeJob(Job{ExecutionTime: 10})

	dag.Link(j1, j2)
	dag.Link(j2, j3)
	dag.Link(j1, j4)
	dag.Link(j4, j5)
	dag.Link(j3, j5)

	opts := ScheduleOpts{
		Workers: 1,
	}

	plan, _ := Schedule(dag, opts)

	if len(plan.schedule) != opts.Workers {
		t.Errorf("wrong schedule size, got %v expected %v", len(plan.schedule), opts.Workers)
	}

	if len(plan.scheduled[0]) != 1 {
		t.Errorf("wrong number of scheduled jobs, got %v expected 1", len(plan.scheduled))
	}

	checkSchedule(plan.schedule, t)
}

func TestScheduleDualWorker(t *testing.T) {
	dag := MakeDAG()

	j0 := dag.MakeJob(Job{ExecutionTime: 10})
	j1 := dag.MakeJob(Job{ExecutionTime: 10})
	j2 := dag.MakeJob(Job{ExecutionTime: 10})
	j3 := dag.MakeJob(Job{ExecutionTime: 10})
	j4 := dag.MakeJob(Job{ExecutionTime: 10})
	j5 := dag.MakeJob(Job{ExecutionTime: 10})
	j6 := dag.MakeJob(Job{ExecutionTime: 10})
	j7 := dag.MakeJob(Job{ExecutionTime: 10})

	// first branch
	dag.LinkFromValues(j0, j1, 1)
	dag.LinkFromValues(j1, j3, 1)
	dag.LinkFromValues(j3, j5, 1)
	dag.LinkFromValues(j5, j7, 1)

	// second branch
	dag.LinkFromValues(j0, j2, 1)
	dag.LinkFromValues(j2, j4, 1)
	dag.LinkFromValues(j4, j6, 1)

	opts := ScheduleOpts{
		Workers: 2,
	}

	plan, _ := Schedule(dag, opts)

	if len(plan.schedule) != opts.Workers {
		t.Errorf("wrong schedule size, got %v expected %v", len(plan.schedule), opts.Workers)
	}

	if len(plan.scheduled[0])+len(plan.scheduled[1]) != 1 {
		t.Errorf("wrong number of scheduled jobs")
	}

	checkSchedule(plan.schedule, t)
}

func TestConsumerAPI(t *testing.T) {

	dag := MakeDAG()

	j0 := dag.MakeJob(Job{ExecutionTime: 2})
	j1 := dag.MakeJob(Job{ExecutionTime: 2})
	j2 := dag.MakeJob(Job{ExecutionTime: 2})
	j3 := dag.MakeJob(Job{ExecutionTime: 2})
	j4 := dag.MakeJob(Job{ExecutionTime: 2})
	j5 := dag.MakeJob(Job{ExecutionTime: 2})
	j6 := dag.MakeJob(Job{ExecutionTime: 2})
	j7 := dag.MakeJob(Job{ExecutionTime: 2})

	// first branch
	dag.LinkFromValues(j0, j1, 1)
	dag.LinkFromValues(j1, j3, 1)
	dag.LinkFromValues(j3, j5, 1)
	dag.LinkFromValues(j5, j7, 1)

	// second branch
	dag.LinkFromValues(j0, j2, 1)
	dag.LinkFromValues(j2, j4, 1)
	dag.LinkFromValues(j4, j6, 1)
	dag.LinkFromValues(j6, j7, 1)

	opts := ScheduleOpts{
		Workers: 2,
	}

	plan, _ := Schedule(dag, opts)

	checkSchedule(plan.schedule, t)

	// define a wait group to avoid termination before completion
	wg := sync.WaitGroup{}

	worker := func(id int) {
		defer wg.Done()

		for j := range plan.NextJobForWorker(id) {

			t.Logf("worker %v started to work on job %v", id, j.ID)

			// do some fake work ..
			time.Sleep(time.Duration(j.ExecutionTime) * time.Second)

			t.Logf("worker %v completed job %v", id, j.ID)

			plan.Done(j.ID)
		}
	}

	// run 2 workers
	go worker(0)
	go worker(1)

	wg.Add(2)
	wg.Wait()
}

func checkSchedule(s schedule, t *testing.T) {

	// check that start time and end time are coherent
	for _, ws := range s {
		for i := 0; i < len(ws)-1; i++ {
			if ws[i].EndTime > ws[i+1].StartTime {
				t.Errorf("job %v end time after job %v start time", ws[i].ID, ws[i+1].ID)
			}
		}
	}

	// print schedule for debug
	t.Logf("[!] first job is missing since is already scheduled for execution")
	for wi, ws := range s {
		t.Logf("worker %v schedule", wi)
		for _, j := range ws {
			t.Logf("job %-5v st %-5v et %-5v", j.ID, j.StartTime, j.EndTime)
		}
	}
}
