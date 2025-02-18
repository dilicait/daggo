package dago

import "testing"

func TestDAG(t *testing.T) {
	dag := MakeDAG()

	j1 := dag.MakeJob(Job{})
	j2 := dag.MakeJob(Job{})
	j3 := dag.MakeJob(Job{})
	j4 := dag.MakeJob(Job{})

	dag.LinkFromValues(j1, j2, 10)
	dag.LinkFromValues(j2, j3, 20)
	dag.LinkFromValues(j1, j4, 30)

	roots := dag.Roots()
	if len(roots) != 1 {
		t.Error("DAG has multiple roots, expected 1")
	}

	root := roots[0]
	rootc := dag.Next(root.ID)
	if len(rootc) != 2 {
		t.Errorf("root has %v next childs, expected 2", len(rootc))
	}

	leaves := dag.Leaves()
	if len(leaves) != 2 {
		t.Errorf("wrong DAG leaves number, got %v expected 2", len(leaves))
	}

	links := dag.Next(j2.ID)
	if len(links) != 1 {
		t.Errorf("bad link number for j2, got %v expected 1", len(links))
	}

	link := links[0]
	if link.JobID != j3.ID {
		t.Errorf("bad link for j2, got %v expected %v", link.JobID, j3.ID)
	}

}
