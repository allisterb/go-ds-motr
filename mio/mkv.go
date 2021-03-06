/*
 * Copyright (c) 2021 Seagate Technology LLC and/or its Affiliates
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For any questions about this software or licensing,
 * please email opensource@seagate.com or cortx-questions@seagate.com.
 *
 * Original author: Andriy Tkachuk <andriy.tkachuk@seagate.com>
 * Original creation date: 29-Apr-2021
 */

package mio

// #include <errno.h> /* EEXIST */
// #include "motr/config.h"
// #include "motr/client.h"
// #include "motr/layout.h" /* m0c_pools_common */
//
// extern struct m0_container container;
//
import "C"

import (
	"errors"
	"fmt"
	"hash/fnv"
	"unsafe"

	ds "github.com/ipfs/go-datastore"
	logging "github.com/ipfs/go-log/v2"

	"github.com/allisterb/go-ds-motr/uint128"
)

// Mkv provides key-value API to Motr
type Mkv struct {
	idxID C.struct_m0_uint128
	idx   *C.struct_m0_idx
}

var log = logging.Logger("motrds")
var hash128 = fnv.New128()

func uint128fid(u C.struct_m0_uint128) (f C.struct_m0_fid) {
	f.f_container = u.u_hi
	f.f_key = u.u_lo
	return f
}

func (mkv *Mkv) idxNew(id string) (err error) {
	mkv.idxID, err = ScanID(id)
	if err != nil {
		return err
	}
	fid := uint128fid(mkv.idxID)
	if C.m0_fid_tget(&fid) != C.m0_dix_fid_type.ft_id {
		return fmt.Errorf("index fid must start with 0x%x in MSByte, for example: 0x%x00000000000123:0x",
			C.m0_dix_fid_type.ft_id, C.m0_dix_fid_type.ft_id)
	}
	mkv.idx = (*C.struct_m0_idx)(C.calloc(1, C.sizeof_struct_m0_idx))
	return nil
}

// Open opens Mkv index for key-value operations.
func (mkv *Mkv) Open(id string, create bool) error {
	if mkv.idx != nil {
		return errors.New("index is already opened")
	}

	err := mkv.idxNew(id)
	if err != nil {
		return err
	}

	C.m0_idx_init(mkv.idx, &C.container.co_realm, &mkv.idxID)

	if create { // Make sure it's created
		var op *C.struct_m0_op
		rc := C.m0_entity_create(nil, &mkv.idx.in_entity, &op)
		if rc != 0 {
			mkv.Close()
			return fmt.Errorf("failed to set create op: %d", rc)
		} else {
			log.Infof("Creating index %s...", id)
		}
		C.m0_op_launch(&op, 1)
		rc = C.m0_op_wait(op, bits(C.M0_OS_FAILED,
			C.M0_OS_STABLE), C.M0_TIME_NEVER)
		if rc == 0 {
			rc = C.m0_rc(op)
		}

		C.m0_op_fini(op)
		C.m0_op_free(op)

		if rc != 0 && rc != -C.EEXIST {
			return fmt.Errorf("index create failed: %d", rc)
		}
	}

	return nil
}

// Close closes Mkv index releasing all the resources
// that were allocated for it.
func (mkv *Mkv) Close() error {
	if mkv.idx == nil {
		return errors.New("index is not opened")
	}
	C.m0_idx_fini(mkv.idx)
	C.free(unsafe.Pointer(mkv.idx))
	mkv.idx = nil

	return nil
}

func (mkv *Mkv) doIdxOp(opcode uint32, key []byte, value []byte,
	update bool) ([]byte, error) {
	if mkv.idx == nil {
		return nil, errors.New("index is not opened")
	}

	var k, v C.struct_m0_bufvec
	if C.m0_bufvec_empty_alloc(&k, 1) != 0 {
		return nil, errors.New("failed to allocate key bufvec")
	}
	defer C.m0_bufvec_free2(&k)

	if opcode == C.M0_IC_PUT || opcode == C.M0_IC_GET {
		if C.m0_bufvec_empty_alloc(&v, 1) != 0 {
			return nil, errors.New("failed to allocate value bufvec")
		}
		if opcode == C.M0_IC_GET {
			defer C.m0_bufvec_free(&v) // cleanup buffer after GET
		} else {
			defer C.m0_bufvec_free2(&v)
		}
	}

	*k.ov_buf = unsafe.Pointer(&key[0])
	*k.ov_vec.v_count = C.ulong(len(key))
	if opcode == C.M0_IC_PUT {
		var p unsafe.Pointer
		if len(value) > 0 {
			p = unsafe.Pointer(&value[0])
		} else {
			p = unsafe.Pointer(&value)
		}
		*v.ov_buf = p
		*v.ov_vec.v_count = C.ulong(len(value))
	}

	vPtr := &v
	if opcode == C.M0_IC_DEL {
		vPtr = nil
	}

	flags := C.uint(0)
	if opcode == C.M0_IC_PUT && update {
		flags = C.M0_OIF_OVERWRITE
	}

	var rcI C.int32_t
	var op *C.struct_m0_op
	rc := C.m0_idx_op(mkv.idx, opcode, &k, vPtr, &rcI, flags, &op)
	if rc != 0 {
		return nil, fmt.Errorf("failed to init index op: %d", rc)
	}

	C.m0_op_launch(&op, 1)
	rc = C.m0_op_wait(op, bits(C.M0_OS_FAILED,
		C.M0_OS_STABLE), C.M0_TIME_NEVER)
	if rc == 0 {
		rc = C.m0_rc(op)
	}
	C.m0_op_fini(op)
	C.m0_op_free(op)

	if rc != 0 {
		return nil, fmt.Errorf("op failed: %d", rc)
	}
	if rcI != 0 {
		return nil, fmt.Errorf("index op failed: %d", rcI)
	}

	if opcode == C.M0_IC_GET {
		value = make([]byte, *v.ov_vec.v_count)
		copy(value, pointer2slice(*v.ov_buf, int(*v.ov_vec.v_count)))
	}

	return value, nil
}

// Put puts key-value into the index.
func (mkv *Mkv) Put(key []byte, value []byte, update bool) error {
	_, err := mkv.doIdxOp(C.M0_IC_PUT, key, value, update)
	log.Debugf("        Put OID %s with size %v bytes to Motr: (%v, %v).", getOIDstr(key), len(value), (err == nil), err)
	return err
}

// Get gets value from the index by key.
func (mkv *Mkv) Get(key []byte) ([]byte, error) {
	value, err := mkv.doIdxOp(C.M0_IC_GET, key, nil, false)
	log.Debugf("        Get OID %s from Motr: (%v, %v).", getOIDstr(key), (err == nil), err)
	return value, err
}

// Delete deletes the record by key.
func (mkv *Mkv) Delete(key []byte) error {
	_, err := mkv.doIdxOp(C.M0_IC_DEL, key, nil, false)
	log.Debugf("        Delete OID %s from Motr: (%v, %v).", getOIDstr(key), (err == nil), err)
	return err
}

func (mkv *Mkv) Has(key []byte) (bool, error) {
	var k, v C.struct_m0_bufvec
	if C.m0_bufvec_empty_alloc(&k, 1) != 0 {
		return false, errors.New("failed to allocate key bufvec")
	}
	defer C.m0_bufvec_free2(&k)

	if C.m0_bufvec_empty_alloc(&v, 1) != 0 {
		return false, errors.New("failed to allocate value bufvec")
	}
	defer C.m0_bufvec_free(&v) // cleanup buffer after GET

	*k.ov_buf = unsafe.Pointer(&key[0])
	*k.ov_vec.v_count = C.ulong(len(key))
	vPtr := &v
	flags := C.uint(0)
	var rcI C.int32_t
	var op *C.struct_m0_op
	rc := C.m0_idx_op(mkv.idx, C.M0_IC_GET, &k, vPtr, &rcI, flags, &op)
	if rc != 0 {
		return false, fmt.Errorf("failed to init index op: %d", rc)
	}

	C.m0_op_launch(&op, 1)
	rc = C.m0_op_wait(op, bits(C.M0_OS_FAILED,
		C.M0_OS_STABLE), C.M0_TIME_NEVER)
	if rc == 0 {
		rc = C.m0_rc(op)
	}
	C.m0_op_fini(op)
	C.m0_op_free(op)

	if rc != 0 {
		return false, fmt.Errorf("op failed: %d", rc)
	} else if rcI != 0 {
		return false, nil
	} else {
		return true, nil
	}
}

func (mkv *Mkv) GetSize(key []byte) (int, error) {
	var k, v C.struct_m0_bufvec
	if C.m0_bufvec_empty_alloc(&k, 1) != 0 {
		return -1, errors.New("failed to allocate key bufvec")
	}
	defer C.m0_bufvec_free2(&k)

	if C.m0_bufvec_empty_alloc(&v, 1) != 0 {
		return -1, errors.New("failed to allocate value bufvec")
	}
	defer C.m0_bufvec_free(&v) // cleanup buffer after GET

	*k.ov_buf = unsafe.Pointer(&key[0])
	*k.ov_vec.v_count = C.ulong(len(key))
	vPtr := &v
	flags := C.uint(0)
	var rcI C.int32_t
	var op *C.struct_m0_op
	rc := C.m0_idx_op(mkv.idx, C.M0_IC_GET, &k, vPtr, &rcI, flags, &op)
	if rc != 0 {
		return -1, fmt.Errorf("failed to init index op: %d", rc)
	}

	C.m0_op_launch(&op, 1)
	rc = C.m0_op_wait(op, bits(C.M0_OS_FAILED,
		C.M0_OS_STABLE), C.M0_TIME_NEVER)
	if rc == 0 {
		rc = C.m0_rc(op)
	}
	C.m0_op_fini(op)
	C.m0_op_free(op)

	if rc != 0 {
		return -1, fmt.Errorf("op failed: %d", rc)
	} else if rcI != 0 {
		return -1, ds.ErrNotFound
	} else {
		var size int = 0
		if vPtr == nil {
			size = 0
		} else {
			size = int(*v.ov_vec.v_count)
		}
		return size, nil
	}
}

/*
func (mkv *Mkv) GetSize(key []byte) (int, error) {
	if v, eget := mkv.Get(key); eget != nil {
		return -1, ds.ErrNotFound
	} else {
		return len(v), nil
	}

}
*/
func getOID(key ds.Key) []byte {
	return hash128.Sum(key.Bytes())
}

func getOIDstr(oid []byte) string {
	u := uint128.FromBytes(oid)
	return fmt.Sprintf("0x%x:0x%x", u.Hi, u.Lo)
}

// vi: sw=4 ts=4 expandtab ai
