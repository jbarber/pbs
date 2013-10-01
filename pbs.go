// Package pbs provides an interface to the C-based TORQUE library.
// The functions present in this package are a thin wrapper around these
// library functions and as such the TORQUE library provides documentation on
// their usage.
//
// The TORQUE library is not thread safe, particulary when it comes to
// reporting errors, and therefore problems *might* arise if you use this
// package with goroutines.
//
// The following functions have not yet been implemented:
/*
   pbs_alterjob
   pbs_alterjobasync
   pbs_manager
   pbs_rescquery
   pbs_rescreserve
   pbs_asyncrunjob
   pbs_selstat
   pbs_sigjobasync
*/
package pbs

// #cgo CFLAGS: -g
// #cgo LDFLAGS: -ltorque
// #include <stdlib.h>
// #include <torque/pbs_error.h>
// #include <torque/pbs_ifl.h>
import "C"
import (
	"errors"
	"unsafe"
)

// BatchStatus represents the batch_status structure
type BatchStatus struct {
	Name       string
	Text       string
	Attributes []Attrib
}

// Attrib represents the attrl and attropl structures
type Attrib struct {
	Name     string
	Resource string
	Value    string
	Op       Operator
}

// Manner defines how the server should be terminated
type Manner int
// Hold defines the type of job hold to place on a job
type Hold string
// MessageStream which output stream should be written to
type MessageStream int
// Operator defines types of logical comparator
type Operator int

const (
	SHUT_IMMEDIATE                 Manner        = C.SHUT_IMMEDIATE
	SHUT_DELAY                     Manner        = C.SHUT_DELAY
	USER_HOLD                      Hold          = C.USER_HOLD
	OTHER_HOLD                     Hold          = C.OTHER_HOLD
	SYSTEM_HOLD                    Hold          = C.SYSTEM_HOLD
	MSG_ERR                        MessageStream = C.MSG_ERR
	MSG_OUT                        MessageStream = C.MSG_OUT
	ATTR_a                         string        = C.ATTR_a
	ATTR_c                         string        = C.ATTR_c
	ATTR_e                         string        = C.ATTR_e
	ATTR_f                         string        = C.ATTR_f
	ATTR_g                         string        = C.ATTR_g
	ATTR_h                         string        = C.ATTR_h
	ATTR_j                         string        = C.ATTR_j
	ATTR_k                         string        = C.ATTR_k
	ATTR_l                         string        = C.ATTR_l
	ATTR_m                         string        = C.ATTR_m
	ATTR_o                         string        = C.ATTR_o
	ATTR_p                         string        = C.ATTR_p
	ATTR_q                         string        = C.ATTR_q
	ATTR_r                         string        = C.ATTR_r
	ATTR_t                         string        = C.ATTR_t
	ATTR_array_id                  string        = C.ATTR_array_id
	ATTR_u                         string        = C.ATTR_u
	ATTR_v                         string        = C.ATTR_v
	ATTR_A                         string        = C.ATTR_A
	ATTR_args                      string        = C.ATTR_args
	ATTR_M                         string        = C.ATTR_M
	ATTR_N                         string        = C.ATTR_N
	ATTR_S                         string        = C.ATTR_S
	ATTR_depend                    string        = C.ATTR_depend
	ATTR_inter                     string        = C.ATTR_inter
	ATTR_stagein                   string        = C.ATTR_stagein
	ATTR_stageout                  string        = C.ATTR_stageout
	ATTR_jobtype                   string        = C.ATTR_jobtype
	ATTR_submit_host               string        = C.ATTR_submit_host
	ATTR_init_work_dir             string        = C.ATTR_init_work_dir
	ATTR_ctime                     string        = C.ATTR_ctime
	ATTR_exechost                  string        = C.ATTR_exechost
	ATTR_execport                  string        = C.ATTR_execport
	ATTR_mtime                     string        = C.ATTR_mtime
	ATTR_qtime                     string        = C.ATTR_qtime
	ATTR_session                   string        = C.ATTR_session
	ATTR_euser                     string        = C.ATTR_euser
	ATTR_egroup                    string        = C.ATTR_egroup
	ATTR_hashname                  string        = C.ATTR_hashname
	ATTR_hopcount                  string        = C.ATTR_hopcount
	ATTR_security                  string        = C.ATTR_security
	ATTR_sched_hint                string        = C.ATTR_sched_hint
	ATTR_substate                  string        = C.ATTR_substate
	ATTR_name                      string        = C.ATTR_name
	ATTR_owner                     string        = C.ATTR_owner
	ATTR_used                      string        = C.ATTR_used
	ATTR_state                     string        = C.ATTR_state
	ATTR_queue                     string        = C.ATTR_queue
	ATTR_server                    string        = C.ATTR_server
	ATTR_maxrun                    string        = C.ATTR_maxrun
	ATTR_maxreport                 string        = C.ATTR_maxreport
	ATTR_total                     string        = C.ATTR_total
	ATTR_comment                   string        = C.ATTR_comment
	ATTR_cookie                    string        = C.ATTR_cookie
	ATTR_qrank                     string        = C.ATTR_qrank
	ATTR_altid                     string        = C.ATTR_altid
	ATTR_etime                     string        = C.ATTR_etime
	ATTR_exitstat                  string        = C.ATTR_exitstat
	ATTR_forwardx11                string        = C.ATTR_forwardx11
	ATTR_submit_args               string        = C.ATTR_submit_args
	ATTR_tokens                    string        = C.ATTR_tokens
	ATTR_netcounter                string        = C.ATTR_netcounter
	ATTR_umask                     string        = C.ATTR_umask
	ATTR_start_time                string        = C.ATTR_start_time
	ATTR_start_count               string        = C.ATTR_start_count
	ATTR_checkpoint_dir            string        = C.ATTR_checkpoint_dir
	ATTR_checkpoint_name           string        = C.ATTR_checkpoint_name
	ATTR_checkpoint_time           string        = C.ATTR_checkpoint_time
	ATTR_checkpoint_restart_status string        = C.ATTR_checkpoint_restart_status
	ATTR_restart_name              string        = C.ATTR_restart_name
	ATTR_comp_time                 string        = C.ATTR_comp_time
	ATTR_reported                  string        = C.ATTR_reported
	ATTR_intcmd                    string        = C.ATTR_intcmd
	ATTR_P                         string        = C.ATTR_P
	ATTR_node_exclusive            string        = C.ATTR_node_exclusive
	ATTR_exec_gpus                 string        = C.ATTR_exec_gpus
	ATTR_J                         string        = C.ATTR_J
	SET                            Operator      = C.SET
	UNSET                          Operator      = C.UNSET
	INCR                           Operator      = C.INCR
	DECR                           Operator      = C.DECR
	EQ                             Operator      = C.EQ
	NE                             Operator      = C.NE
	GE                             Operator      = C.GE
	GT                             Operator      = C.GT
	LE                             Operator      = C.LE
	LT                             Operator      = C.LT
	DFLT                           Operator      = C.DFLT
	MERGE                          Operator      = C.MERGE
	INCR_OLD                       Operator      = C.INCR_OLD
)

func getLastError () error {
    return errors.New(Pbs_strerror(int(C.pbs_errno)))
}

func attrib2attribl(attribs []Attrib) *C.struct_attrl {
	// Empty array returns null pointer
	if len(attribs) == 0 {
		return nil
	}

	first := &C.struct_attrl{
		value:    C.CString(attribs[0].Value),
		resource: C.CString(attribs[0].Resource),
		name:     C.CString(attribs[0].Name),
        op:       uint32(attribs[0].Op),
	}
	tail := first

	for _, attr := range attribs[1:len(attribs)] {
		tail.next = &C.struct_attrl{
			value:    C.CString(attr.Value),
			resource: C.CString(attr.Resource),
			name:     C.CString(attr.Name),
            op:       uint32(attribs[0].Op),
		}
	}

	return first
}

func freeattribl(attrl *C.struct_attrl) {
	for p := attrl; p != nil; p = p.next {
		C.free(unsafe.Pointer(p.name))
		C.free(unsafe.Pointer(p.value))
		C.free(unsafe.Pointer(p.resource))
	}
}

func get_pbs_batch_status(batch_status *_Ctype_struct_batch_status) (batch []BatchStatus) {
	for batch_status != nil {
		temp := []Attrib{}
		for attr := batch_status.attribs; attr != nil; attr = attr.next {
			temp = append(temp, Attrib{
				Name:     C.GoString(attr.name),
				Resource: C.GoString(attr.resource),
				Value:    C.GoString(attr.value),
			})
		}

		batch = append(batch, BatchStatus{
			Name:       C.GoString(batch_status.name),
			Text:       C.GoString(batch_status.text),
			Attributes: temp,
		})

		batch_status = batch_status.next
	}
	return batch
}

func sptr(p uintptr) *C.char {
	return *(**C.char)(unsafe.Pointer(p))
}

func cstrings(x **C.char) []string {
	var s []string
	for p := uintptr(unsafe.Pointer(x)); sptr(p) != nil; p += unsafe.Sizeof(uintptr(0)) {
		s = append(s, C.GoString(sptr(p)))
	}
	return s
}

func freeCstrings(x **C.char) {
	for p := uintptr(unsafe.Pointer(x)); sptr(p) != nil; p += unsafe.Sizeof(uintptr(0)) {
		C.free(unsafe.Pointer(sptr(p)))
	}
}

func Pbs_checkpointjob(handle int, id string, extend string) error {
	s := C.CString(id)
	defer C.free(unsafe.Pointer(s))

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	ret := C.pbs_checkpointjob(C.int(handle), s, e)
	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}

	return nil
}

// Pbs_connect makes a connection to server, or if server is an empty string, the default server. The returned handle is used by subsequent calls to the functions in this package to identify the server.
func Pbs_connect(server string) (int, error) {
	str := C.CString(server)
	defer C.free(unsafe.Pointer(str))

	handle := C.pbs_connect(str)
	if handle < 0 {
		return 0, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}

	return int(handle), nil
}

// Pbs_default reports the default torque server
func Pbs_default() string {
	// char* from pbs_default is statically allocated, so can't be freed
	return C.GoString(C.pbs_default())
}

// Pbs_deljob deletes a job on the server
func Pbs_deljob(handle int, id string, extend string) error {
	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	s := C.CString(id)
	defer C.free(unsafe.Pointer(s))

	ret := C.pbs_deljob(C.int(handle), s, e)
	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}

	return nil
}

func Pbs_disconnect(handle int) error {
	ret := C.pbs_disconnect(C.int(handle))
	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	return nil
}

func Pbs_fbserver() string {
	// char* from pbs_fbserver is statically allocated, so can't be freed
	return C.GoString(C.pbs_fbserver())
}

func Pbs_get_server_list() string {
	// char* from pbs_get_server_list is statically allocated, so can't be freed
	return C.GoString(C.pbs_get_server_list())
}

func Pbs_geterrmsg(handle int) string {
	s := C.pbs_geterrmsg(C.int(handle))
	// char* from pbs_geterrmsg is statically allocated, so can't be freed
	return C.GoString(s)
}

func Pbs_gpumode (handle int, mom_node string, gpu_id int, gpu_mode int) error {
	m := C.CString(mom_node)
	defer C.free(unsafe.Pointer(m))

    ret := C.pbs_gpumode(C.int(handle), m, C.int(gpu_id), C.int(gpu_mode))
    if ret != 0 {
		return getLastError()
    }
    return nil
}

/*
// pbs_gpureset not declared in pbs_ifl.h for 3.0.0
func Pbs_gpureset (handle int, mom_node string, gpu_id int, ecc_perm int, ecc_vol int) error {
	m := C.CString(mom_node)
	defer C.free(unsafe.Pointer(m))

    ret := C.pbs_gpureset(C.int(handle), m, C.int(gpu_id), C.int(ecc_perm), C.int(ecc_vol))
    if ret != 0 {
		return getLastError()
    }
    return nil
}
*/

func Pbs_holdjob(handle int, id string, holdType Hold, extend string) error {
	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	s := C.CString(id)
	defer C.free(unsafe.Pointer(s))

	ht := C.CString(string(holdType))
	defer C.free(unsafe.Pointer(ht))

	ret := C.pbs_holdjob(C.int(handle), s, ht, e)
	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}

	return nil
}

func Pbs_locjob(handle int, id string) (string, error) {
	s := C.CString(id)
	defer C.free(unsafe.Pointer(s))

	ret := C.pbs_locjob(C.int(handle), s, nil)
	if ret == nil {
		return "", errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	defer C.free(unsafe.Pointer(ret))

	return C.GoString(ret), nil
}

func Pbs_movejob(handle int, id string, destination string, extend string) error {
    i := C.CString(id)
    defer C.free(unsafe.Pointer(i))

    d := C.CString(destination)
    defer C.free(unsafe.Pointer(d))

    e := C.CString(extend)
    defer C.free(unsafe.Pointer(e))

    ret := C.pbs_movejob(C.int(handle), i, d, e)
    if ret != 0 {
        return errors.New(Pbs_strerror(int(C.pbs_errno)))
    }

    return nil
}

func Pbs_msgjob(handle int, id string, file MessageStream, message string, extend string) error {
	s := C.CString(id)
	defer C.free(unsafe.Pointer(s))

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	m := C.CString(message)
	defer C.free(unsafe.Pointer(m))

	ret := C.pbs_msgjob(C.int(handle), s, C.int(file), m, e)
	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	return nil
}

func Pbs_orderjob(handle int, job_id1 string, job_id2, extend string) error {
	j1 := C.CString(job_id1)
	defer C.free(unsafe.Pointer(j1))

	j2 := C.CString(job_id1)
	defer C.free(unsafe.Pointer(j2))

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	ret := C.pbs_orderjob(C.int(handle), j1, j2, e)
	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	return nil
}

func Pbs_rerunjob(handle int, id string, extend string) error {
	s := C.CString(id)
	defer C.free(unsafe.Pointer(s))

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	ret := C.pbs_rerunjob(C.int(handle), s, e)

	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	return nil
}

func Avail(handle int, resc string) string {
	r := C.CString(resc)
	defer C.free(unsafe.Pointer(r))

	c := C.avail(C.int(handle), r)
	//defer C.free(unsafe.Pointer(c))

	return C.GoString(c)
}

func Totpool(handle int, update int) (int, error) {
	ret := int(C.totpool(C.int(handle), C.int(update)))
	if ret < 0 {
		return ret, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	return ret, nil
}

func Usepool(handle int, update int) (int, error) {
	ret := int(C.usepool(C.int(handle), C.int(update)))
	if ret < 0 {
		return ret, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	return ret, nil
}

func Pbs_rlsjob(handle int, id string, holdType Hold, extend string) error {
	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	s := C.CString(id)
	defer C.free(unsafe.Pointer(s))

	ht := C.CString(string(holdType))
	defer C.free(unsafe.Pointer(ht))

	ret := C.pbs_rlsjob(C.int(handle), s, ht, e)
	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}

	return nil
}

func Pbs_runjob(handle int, id string, location string, extend string) error {
	i := C.CString(id)
	defer C.free(unsafe.Pointer(i))

	l := C.CString(location)
	defer C.free(unsafe.Pointer(l))

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	ret := C.pbs_runjob(C.int(handle), i, l, e)
	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	return nil
}

// Pbs_selectjob returns a list of jobs
func Pbs_selectjob(handle int, attrib []Attrib, extend string) ([]string, error) {
	// Torque's implementation of pbs_selectjob() is broken and only works
	// accidentally - they allocate a single block of memory (which is
	// oversized) for the jobids and then copy them into it.
	// Because only a single malloc() is used you only need to free() the
	// char** returned by pbs_selectjob().
	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	a := attrib2attribl(attrib)
	defer freeattribl(a)

	p := C.pbs_selectjob(C.int(handle), (*C.struct_attropl)(unsafe.Pointer(a)), e)
	if p == nil {
		return nil, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	defer C.free(unsafe.Pointer(p))

	jobArray := cstrings(p)
	return jobArray, nil
}

func Pbs_sigjob(handle int, id string, signal string, extend string) error {
	i := C.CString(id)
	defer C.free(unsafe.Pointer(i))

	s := C.CString(signal)
	defer C.free(unsafe.Pointer(s))

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	ret := C.pbs_sigjob(C.int(handle), i, s, e)
	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	return nil
}

/*
// pbs_stagein not declared in pbs_ifl.h 3.0.0
func Pbs_stagein(handle int, id string, location string, extend string) error {
	i := C.CString(id)
	defer C.free(unsafe.Pointer(i))

	l := C.CString(location)
	defer C.free(unsafe.Pointer(l))

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

    ret := C.pbs_stagein(C.int(handle), i, l, e)
	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	return nil
}
*/

func Pbs_statjob(handle int, id string, attribs []Attrib, extend string) ([]BatchStatus, error) {
	i := C.CString(id)
	defer C.free(unsafe.Pointer(i))

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	a := attrib2attribl(attribs)
	defer freeattribl(a)

	batch_status := C.pbs_statjob(C.int(handle), i, a, e)

	if batch_status == nil {
		return nil, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	defer C.pbs_statfree(batch_status)

	batch := get_pbs_batch_status(batch_status)

	return batch, nil
}

func Pbs_statnode(handle int, id string, attribs []Attrib, extend string) ([]BatchStatus, error) {
	i := C.CString(id)
	defer C.free(unsafe.Pointer(i))

	a := attrib2attribl(attribs)
	defer freeattribl(a)

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	batch_status := C.pbs_statnode(C.int(handle), i, a, e)

	if batch_status == nil {
		return nil, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	defer C.pbs_statfree(batch_status)

	batch := get_pbs_batch_status(batch_status)

	return batch, nil
}

func Pbs_statque(handle int, id string, attribs []Attrib, extend string) ([]BatchStatus, error) {
	i := C.CString(id)
	defer C.free(unsafe.Pointer(i))

	a := attrib2attribl(attribs)
	defer freeattribl(a)

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	batch_status := C.pbs_statque(C.int(handle), i, a, e)

	if batch_status == nil {
		return nil, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	defer C.pbs_statfree(batch_status)

	batch := get_pbs_batch_status(batch_status)

	return batch, nil
}

func Pbs_statserver(handle int, attribs []Attrib, extend string) ([]BatchStatus, error) {
	a := attrib2attribl(attribs)
	defer freeattribl(a)

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	batch_status := C.pbs_statserver(C.int(handle), a, e)

	if batch_status == nil {
		return nil, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	defer C.pbs_statfree(batch_status)

	batch := get_pbs_batch_status(batch_status)

	return batch, nil
}

func Pbs_strerror(errno int) string {
	// char* from pbs_strerror is statically allocated, so can't be freed
	return C.GoString(C.pbs_strerror(C.int(errno)))
}

func Pbs_submit(handle int, attribs []Attrib, script string, destination string, extend string) (string, error) {
	a := attrib2attribl(attribs)
	defer freeattribl(a)

	s := C.CString(script)
	defer C.free(unsafe.Pointer(s))

	d := C.CString(destination)
	defer C.free(unsafe.Pointer(d))

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	jobid := C.pbs_submit(C.int(handle), (*C.struct_attropl)(unsafe.Pointer(a)), s, d, e)
	if jobid == nil {
		return "", errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	defer C.free(unsafe.Pointer(jobid))

	return C.GoString(jobid), nil
}

func Pbs_terminate(handle int, manner Manner, extend string) error {
	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	ret := C.pbs_terminate(C.int(handle), C.int(int(manner)), e)
	if ret != 0 {
		return errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	return nil
}
