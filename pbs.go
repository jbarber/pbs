package pbs

/* pbs functions:
   pbs_alterjob
   pbs_alterjobasync
   pbs_checkpointjob    -  done
   pbs_connect          -  done
   pbs_default          -  done
   pbs_deljob           -  done
   pbs_disconnect       -  done
   pbs_fbserver         -  done
   pbs_get_server_list  -  done
   pbs_geterrmsg        -  done
   pbs_gpumode
   pbs_gpureset
   pbs_holdjob          -  done (untested)
   pbs_locjob           -  done
   pbs_manager
   pbs_movejob
   pbs_msgjob           -  done (untested)
   pbs_orderjob         -  done (untested)
   pbs_rerunjob         -  done
   pbs_rescquery
   avail
   totpool              -  done
   usepool              -  done
   pbs_rescreserve
   pbs_rlsjob           -  done (untested)
   pbs_runjob           -  done (untested)
   pbs_asyncrunjob
   pbs_selectjob        -  partially done
   pbs_selstat
   pbs_sigjob           -  done
   pbs_sigjobasync
   pbs_stagein
   pbs_statjob          -  partially done
   pbs_statnode         -  partially done
   pbs_statque          -  partially done
   pbs_statserver       -  partially done
   pbs_strerror         -  done
   pbs_submit           -  partially done
   pbs_terminate        -  done
*/

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

type BatchStatus struct {
	Name       string
	Text       string
	Attributes []Attrib
}
type Attrib struct {
	Name     string
	Resource string
	Value    string
}
type Manner int
type Hold string
type MessageStream int

const (
	SHUT_IMMEDIATE Manner        = C.SHUT_IMMEDIATE
	SHUT_DELAY     Manner        = C.SHUT_DELAY
	USER_HOLD      Hold          = C.USER_HOLD
	OTHER_HOLD     Hold          = C.OTHER_HOLD
	SYSTEM_HOLD    Hold          = C.SYSTEM_HOLD
	MSG_ERR        MessageStream = C.MSG_ERR
	MSG_OUT        MessageStream = C.MSG_OUT
)

func get_pbs_batch_status(batch_status *_Ctype_struct_batch_status) (batch []BatchStatus) {
	for batch_status != nil {
		temp := []Attrib{}
		for attr := batch_status.attribs; attr.next != nil; attr = attr.next {
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

func Pbs_connect(server string) (int, error) {
	str := C.CString(server)
	defer C.free(unsafe.Pointer(str))
	handle := C.pbs_connect(str)

	if handle < 0 {
		return 0, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	return int(handle), nil
}

func Pbs_default() string {
    // char* from pbs_default is statically allocated, so can't be freed
    return C.GoString(C.pbs_default())
}

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
	defer C.free(unsafe.Pointer(s))
	return C.GoString(s)
}

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

func Pbs_selectjob(handle int) ([]string, error) {
    // Torque's implementation of pbs_selectjob() is hideously broken and it
    // only works accidentally - they allocate a single block of memory (which is
    // oversized) for the jobids and then copy them into it.
    // Because only a single malloc() is used you only need to free() the
    // char** returned by pbs_selectjob().

	p := C.pbs_selectjob(C.int(handle), nil, nil)
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

func Pbs_statjob(handle int, id string) ([]BatchStatus, error) {
	s := C.CString(id)
	defer C.free(unsafe.Pointer(s))

	batch_status := C.pbs_statjob(C.int(handle), s, nil, nil)

	if batch_status == nil {
		return nil, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	defer C.pbs_statfree(batch_status)

	batch := get_pbs_batch_status(batch_status)

	return batch, nil
}

func Pbs_statnode(handle int, id string) ([]BatchStatus, error) {
	s := C.CString(id)
	defer C.free(unsafe.Pointer(s))

	batch_status := C.pbs_statnode(C.int(handle), s, nil, nil)

	if batch_status == nil {
		return nil, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	defer C.pbs_statfree(batch_status)

	batch := get_pbs_batch_status(batch_status)

	return batch, nil
}

func Pbs_statque(handle int) ([]BatchStatus, error) {
	batch_status := C.pbs_statque(C.int(handle), nil, nil, nil)

	if batch_status == nil {
		return nil, errors.New(Pbs_strerror(int(C.pbs_errno)))
	}
	defer C.pbs_statfree(batch_status)

	batch := get_pbs_batch_status(batch_status)

	return batch, nil
}

func Pbs_statserver(handle int) ([]BatchStatus, error) {
	batch_status := C.pbs_statserver(C.int(handle), nil, nil)

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

func Pbs_submit(handle int, script string, destination string, extend string) (string, error) {
	s := C.CString(script)
	defer C.free(unsafe.Pointer(s))

	d := C.CString(destination)
	defer C.free(unsafe.Pointer(d))

	e := C.CString(extend)
	defer C.free(unsafe.Pointer(e))

	jobid := C.pbs_submit(C.int(handle), nil, s, d, e)
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
