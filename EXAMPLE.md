```go
package main

import (
	"fmt"
	"log"
	"github.com/jbarber/pbs"
)

func main() {
    server := "localhost"
	handle, err := pbs.Pbs_connect(server)
	if err != nil {
		log.Fatalf("Can't connect to %s: %s\n", server, err)
	}

	defer func() {
		err = pbs.Pbs_disconnect(handle)
		if err != nil {
			log.Fatal(err)
		}
	}()

    fmt.Printf("errmsg is: %s\n", pbs.Pbs_geterrmsg(handle))

	// get the default server name
    {
        server := pbs.Pbs_default()
        if err != nil {
            log.Fatal(err)
        }
        fmt.Printf("Default server is: %s\n", server)
    }

	// Get the servers
    {
        servers := pbs.Pbs_get_server_list()
        fmt.Printf("Servers are: %s\n", servers)
    }

	// Get the fallback server
    {
        fallbackServer := pbs.Pbs_fbserver()
        if fallbackServer != "" {
            fmt.Printf("Fallback server: %s\n", fallbackServer)
        }
    }

    // See if required resources available
    {
        avail := pbs.Avail(handle, "nodes=1")
        fmt.Printf("nodes=1: %s\n", avail)
    }

    // See how many nodes are present
    {
        total, err := pbs.Totpool(handle, 1)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Printf("Total nodes: %d\n", total)
    }

    // See how many nodes are in use
    {
        use, err := pbs.Usepool(handle, 1)
        if err != nil {
            log.Fatal(err)
        }
        fmt.Printf("Used nodes: %d\n", use)
    }

    // Get server statistics
    {
		attribs, err := pbs.Pbs_statserver(handle, nil, "")
		if err != nil {
			log.Fatal(err)
		}

		for _, server := range attribs {
			fmt.Printf("%s (%s)\n", server.Name, server.Text)
			for _, attr := range server.Attributes {
				fmt.Printf("  %s (%s): %s\n", attr.Name, attr.Resource, attr.Value)
			}
		}
    }

    // Get all queue statistics
    {
        queues, err := pbs.Pbs_statque(handle, "", []pbs.Attrib{}, "")
		if err != nil {
			log.Fatal(err)
		}

		for _, queue := range queues {
			fmt.Printf("%s (%s)\n", queue.Name, queue.Text)
			for _, attr := range queue.Attributes {
				fmt.Printf("  %s (%s): %s\n", attr.Name, attr.Resource, attr.Value)
			}
		}
    }

    // Submit a job
    jobid, err := pbs.Pbs_submit(handle, []pbs.Attrib{}, "test.sh", "", "")
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Submitted jobid: %s\n", jobid)

	// List jobs
	{
		jobs, err := pbs.Pbs_selectjob(handle, nil, "")
		if err != nil {
			log.Fatal(err)
		}
		for _, v := range jobs {
			j, err := pbs.Pbs_statjob(handle, v, []pbs.Attrib{pbs.Attrib{Name: pbs.ATTR_owner}}, "")
			if err != nil {
				log.Fatal(err)
			}

			for _, status := range j {
				fmt.Printf("%s (%s)\n", status.Name, status.Text)
				for _, attr := range status.Attributes {
					fmt.Printf("  %s (%s): %s\n", attr.Name, attr.Resource, attr.Value)
				}
			}
		}
	}

    {
        err := pbs.Pbs_holdjob(handle, jobid, pbs.USER_HOLD, "")
        if err != nil {
            log.Fatal(err)
        }
    }

    {
        err = pbs.Pbs_checkpointjob(handle, jobid, "")
        if err != nil {
            log.Fatal(err)
        }
    }

    {
        loc, err := pbs.Pbs_locjob(handle, jobid)
        if err != nil {
            log.Fatalf("Failed to get job location: %s\n", err)
        } else {
            fmt.Printf("Job location: %s\n", loc)
        }
    }

    {
		err = pbs.Pbs_terminate(handle, pbs.SHUT_DELAY, "")
		if err != nil {
			log.Fatalf("Failed to stop server: %s\n", err)
		}
	}

    // List nodes and their attributes
    {
		attribs, err := pbs.Pbs_statnode(handle, "", nil, "")
		if err != nil {
			log.Println(err)
		}
		for _, server := range attribs {
			fmt.Printf("%s (%s)\n", server.Name, server.Text)
			for _, attr := range server.Attributes {
				fmt.Printf("  %s (%s): %s\n", attr.Name, attr.Resource, attr.Value)
			}
		}
    }

    {
        err = pbs.Pbs_msgjob(handle, jobid, pbs.MSG_OUT, "test", "")
        if err != nil {
            log.Println(err)
        }
    }

    {
        err = pbs.Pbs_sigjob(handle, jobid, "SIGUSR1", "")
        if err != nil {
            log.Println(err)
        }
    }

    {
        err = pbs.Pbs_deljob(handle, jobid, "")
        if err != nil {
            log.Println(err)
        }
    }
}
```
