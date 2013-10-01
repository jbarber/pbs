package pbs

import (
	"os"
	"os/user"
	"testing"
)

var server = "localhost"
var scriptPath = "test.sh"

func logAttribute(t *testing.T, attr Attrib) {
	if attr.Resource != "" {
		t.Logf("  %s.%s: %s\n", attr.Name, attr.Resource, attr.Value)
	} else {
		t.Logf("  %s: %s\n", attr.Name, attr.Value)
	}
}

func createSubmitScript(t *testing.T) {
	fo, err := os.Create(scriptPath)
	if err != nil {
		t.Fatalf("Couldn't create submit script %s: %s", scriptPath, err)
	}

	defer func() {
		if err := fo.Close(); err != nil {
			t.Fatalf("Couldn't close submit script: %s\n", err)
		}
	}()

	_, err = fo.WriteString("#!/bin/bash\nsleep 10\n")
	if err != nil {
		t.Fatalf("Couldn't write submit script: %s\n", err)
	}

	err = fo.Sync()
	if err != nil {
		t.Fatalf("Couldn't sync submit script: %s\n", err)
	}
}

func getCurrentUser(t *testing.T) string {
	user, err := user.Current()
	if err != nil {
		t.Fatalf("Couldn't get current user: %s", err)
	}
	return user.Username
}

func TestConnect(t *testing.T) {
	handle, err := Pbs_connect(server)
	if err != nil {
		t.Fatalf("Connect to %s failed: %s\n", server, err)
	}

	errmsg := Pbs_geterrmsg(handle)
	if errmsg != "" {
		t.Fatalf("errmsg is: %s\n", errmsg)
	}

	err = Pbs_disconnect(handle)
	if err != nil {
		t.Errorf("Disconnecting failed: %s\n", err)
	}

	errmsg = Pbs_geterrmsg(handle)
	if errmsg != "" {
		t.Errorf("errmsg is: %s\n", errmsg)
	}
}

func TestSubmit(t *testing.T) {
	handle, err := Pbs_connect(server)
	if err != nil {
		t.Fatalf("Connect to %s failed: %s\n", server, err)
	}

	defer func() {
		err = Pbs_disconnect(handle)
		if err != nil {
			t.Errorf("Disconnect failed: %s\n", err)
		}
	}()

	createSubmitScript(t)
	defer func() {
		err := os.Remove(scriptPath)
		if err != nil {
			t.Fatalf("Couldn't remove submit script %s: %s", scriptPath, err)
		}
	}()

	jobid, err := Pbs_submit(handle, []Attrib{}, scriptPath, "", "")
	if err != nil {
		t.Errorf("Job submission failed: %s\n", err)
	} else {
		t.Logf("Submitted jobid: %s\n", jobid)
	}
}

func TestListJobs(t *testing.T) {
	handle, err := Pbs_connect(server)
	if err != nil {
		t.Fatalf("Connect to %s failed: %s\n", server, err)
	}

	defer func() {
		err = Pbs_disconnect(handle)
		if err != nil {
			t.Errorf("Disconnect failed: %s\n", err)
		}
	}()

	filter := []Attrib{
		Attrib{
			Name:  ATTR_u,
			Value: getCurrentUser(t),
			Op:    EQ,
		},
	}
	jobs, err := Pbs_selectjob(handle, filter, "")
	if err != nil {
		t.Log(Pbs_geterrmsg(handle))
		t.Fatalf("Couldn't get list of jobs: %s\n", err)
	}

	for _, id := range jobs {
		j, err := Pbs_statjob(handle, id, []Attrib{}, "")
		if err != nil {
			t.Fatalf("Couldn't get job attributes for %s: %s\n", id, err)
		}

		for _, status := range j {
			t.Logf("%s (%s)\n", status.Name, status.Text)
			for _, attr := range status.Attributes {
				logAttribute(t, attr)
			}
		}
	}
}

func TestServerStatistics(t *testing.T) {
	handle, err := Pbs_connect(server)
	if err != nil {
		t.Fatalf("Connect to %s failed: %s\n", server, err)
	}

	defer func() {
		err = Pbs_disconnect(handle)
		if err != nil {
			t.Errorf("Disconnect failed: %s\n", err)
		}
	}()

	attribs, err := Pbs_statserver(handle, nil, "")
	if err != nil {
		t.Errorf("Couldn't get server statistics: %s\n", err)
	}

	for _, server := range attribs {
		t.Logf("%s (%s)\n", server.Name, server.Text)
		for _, attr := range server.Attributes {
			logAttribute(t, attr)
		}
	}
}

func TestQueueStatistics(t *testing.T) {
	handle, err := Pbs_connect(server)
	if err != nil {
		t.Fatalf("Connect to %s failed: %s\n", server, err)
	}

	defer func() {
		err = Pbs_disconnect(handle)
		if err != nil {
			t.Errorf("Disconnect failed: %s\n", err)
		}
	}()

	queues, err := Pbs_statque(handle, "", []Attrib{}, "")
	if err != nil {
		t.Fatalf("Couldn't get queue statistics: %s\n", err)
	}

	for _, queue := range queues {
		t.Logf("%s (%s)\n", queue.Name, queue.Text)
		for _, attr := range queue.Attributes {
			logAttribute(t, attr)
		}
	}
}

func TestResources(t *testing.T) {
	handle, err := Pbs_connect(server)
	if err != nil {
		t.Fatalf("Connect to %s failed: %s\n", server, err)
	}

	defer func() {
		err = Pbs_disconnect(handle)
		if err != nil {
			t.Errorf("Disconnect failed: %s\n", err)
		}
	}()

	// See if required resources available
	{
		avail := Avail(handle, "nodes=1")
		t.Logf("nodes=1: %s\n", avail)
	}

	// See how many nodes are present
	{
		total, err := Totpool(handle, 1)
		if err != nil {
			t.Fatalf("Couldn't get number of nodes: %s\n", err)
		}
		t.Logf("Total nodes: %d\n", total)
	}

	// See how many nodes are in use
	{
		use, err := Usepool(handle, 1)
		if err != nil {
			t.Fatalf("Couldn't get number of used nodes: %s", err)
		}
		t.Logf("Used nodes: %d\n", use)
	}
}
