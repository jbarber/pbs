# pbs - Go interface to the Torque resource manager library

This is a thin Go wrapper around the C library for the Torque resource manager.

Documentation about the functions can be found in the man pages of the Torque
library.

## Install

    go get github.com/jbarber/pbs

## Documentation

    go doc github.com/jbarber/pbs

## Usage

    package main

    import (
        "pbs"
        "log"
    )

    func main() {
        handle, err := pbs.Pbs_connect("torque.example.com")
        if err != nil {
            log.Fatal("Couldn't connect to server: %s", err)
        }

        defer func() {
            err = Pbs_disconnect(handle)
            if err != nil {
                log.Fatal("Disconnect failed: %s\n", err)
            }
        }()

        jobid, err := pbs.Pbs_submit(handle, nil, "test.sh", "")
        if err != nil {
            log.Fatal("Job submission failed: %s\n", err)
        }

        // ...
    }

## Testing

A test suite is present, it requires a running Torque server which accepts jobs
from the user running the tests. If you have this, just run:

    go test github.com/jbarber/pbs

The test suite also provides examples of how to use the functions.
