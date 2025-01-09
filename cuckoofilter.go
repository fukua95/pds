package pds

import (
	"math"

	"github.com/aviddiviner/go-murmur"
)

type cuckooHash uint64
type fingerprint uint8

const nullFp fingerprint = 0

type bucket struct {
	slots []fingerprint
}

type subCF struct {
	bucketNum  uint64
	bucketSize uint16
	buckets    []bucket // all buckets have the same size
}

// A scalable cuckoo filter.
type CuckooFilter struct {
	bucketNum  uint64
	bucketSize uint16
	itemNum    uint64
	deleteNum  uint64
	maxIter    uint16
	expansion  uint16
	filterNum  uint16
	filters    []subCF
}

type params struct {
	h1 cuckooHash
	h2 cuckooHash
	fp fingerprint
}

func buildParams(data []byte) params {
	hash := murmur.MurmurHash64A(data, 0)
	fp := fingerprint(hash%255 + 1)
	return params{
		h1: cuckooHash(hash),
		h2: altHash(fp, cuckooHash(hash)),
		fp: fp,
	}
}

// from the cuckoo filter paper: https://www.cs.cmu.edu/~dga/papers/cuckoo-conext2014.pdf
// when we insert a value `v`, set:
// fp = fingerprint_hash(v)
// h1 = hash(v)
// h2 = hash(v) ^ (fp (* a value))
// so we can know h1 if we know fp and h2: h1 = h2 ^ (fp (* a value))
func altHash(fp fingerprint, index cuckooHash) cuckooHash {
	return index ^ (cuckooHash(fp) * 0x5bd1e995)
}

// methods of bucket

func makeBucket(size uint16) bucket {
	return bucket{
		slots: make([]fingerprint, size),
	}
}

func (b *bucket) find(fp fingerprint) bool {
	for _, v := range b.slots {
		if v == fp {
			return true
		}
	}
	return false
}

func (b *bucket) delete(fp fingerprint) bool {
	for i, v := range b.slots {
		if v == fp {
			b.slots[i] = nullFp
			return true
		}
	}
	return false
}

func (b *bucket) count(fp fingerprint) uint16 {
	res := uint16(0)
	for _, v := range b.slots {
		if v == fp {
			res++
		}
	}
	return res
}

func (b *bucket) findAvailableSlot() (*fingerprint, bool) {
	for i := range b.slots {
		if b.slots[i] == nullFp {
			return &b.slots[i], true
		}
	}
	return nil, false
}

// methods of subCF

func (s *subCF) bucketIndex(hash cuckooHash) uint32 {
	return uint32((uint64(hash) % s.bucketNum))
}

func (s *subCF) find(params params) bool {
	p1, p2 := s.bucketIndex(params.h1), s.bucketIndex(params.h2)
	return s.buckets[p1].find(params.fp) || s.buckets[p2].find(params.fp)
}

func (s *subCF) delete(params params) bool {
	p1, p2 := s.bucketIndex(params.h1), s.bucketIndex(params.h2)
	return s.buckets[p1].delete(params.fp) || s.buckets[p2].delete(params.fp)
}

func (s *subCF) count(params params) uint16 {
	p1, p2 := s.bucketIndex(params.h1), s.bucketIndex(params.h2)
	return s.buckets[p1].count(params.fp) + s.buckets[p2].count(params.fp)
}

func (s *subCF) findAvailableSlot(params params) (*fingerprint, bool) {
	p1, p2 := s.bucketIndex(params.h1), s.bucketIndex(params.h2)
	for _, p := range []uint32{p1, p2} {
		if slot, ok := s.buckets[p].findAvailableSlot(); ok {
			return slot, true
		}
	}
	return nil, false
}

// methods of CuckooFilter

func next2N(n uint64) uint64 {
	n--
	n |= n >> 1
	n |= n >> 2
	n |= n >> 4
	n |= n >> 8
	n |= n >> 16
	n |= n >> 32
	n++
	return n
}

/*
 * @capacity
 *	capacity is the number of elements you expect to have in the cuckoo filter.
 *
 * @bucketSize
 *  number of items in each bucket.
 *  its general value: 1, 2, 4
 *  a higher bucket size value improves the fill rate but also causes a higher error rate and slightly slower performance.
 *  error_rate = (bucket_size * hash_function_num) / 2 ^ fingerprint_size = (bucket_size * 2) / 256
 *  so when bucket size is 1, error rate = 0.78% is the minimal false positive rate we can achieve.
 *
 * @expansion
 *  the scaling factor.
 *  its general value is 1.
 *
 * @maxIter
 *  the number of attempts to find a slot for the incoming fingerprint.
 *  its default value is 20.
 */
func NewCuckooFilter(capacity uint64, bucketSize uint16, maxIter uint16, expansion uint16) *CuckooFilter {
	filter := &CuckooFilter{
		expansion:  uint16(next2N(uint64(expansion))),
		bucketSize: bucketSize,
		maxIter:    maxIter,
		bucketNum:  next2N(capacity / uint64(bucketSize)),
		filterNum:  0,
	}
	if filter.bucketNum == 0 {
		filter.bucketNum = 1
	}
	filter.grow()
	return filter
}

func (cf *CuckooFilter) grow() {
	growth := math.Pow(float64(cf.expansion), float64(cf.filterNum))

	curFilter := subCF{
		bucketSize: cf.bucketSize,
		bucketNum:  cf.bucketNum * uint64(growth),
	}
	curFilter.buckets = make([]bucket, curFilter.bucketNum)
	for i := range curFilter.buckets {
		curFilter.buckets[i] = makeBucket(curFilter.bucketSize)
	}

	cf.filters = append(cf.filters, curFilter)
	cf.filterNum++
}

type cuckooInsertStatus int8

const (
	cuckooInserted       cuckooInsertStatus = 1
	cuckooAlreadyExist   cuckooInsertStatus = 2
	cuckooNospace        cuckooInsertStatus = 3
	cuckooMemAllocFailed cuckooInsertStatus = 4
)

func (cf *CuckooFilter) evictAndInsert(params params) cuckooInsertStatus {
	curFilter := &cf.filters[cf.filterNum-1]
	fp := params.fp
	victimIx := uint32(0)
	p := uint64(params.h1) % curFilter.bucketNum

	for i := 0; i < int(cf.maxIter); i++ {
		bucket := &curFilter.buckets[p]
		bucket.slots[victimIx], fp = fp, bucket.slots[victimIx]
		p = uint64(altHash(fp, cuckooHash(p))) % curFilter.bucketNum
		if slot, ok := bucket.findAvailableSlot(); ok {
			*slot = fp
			return cuckooInserted
		}
		victimIx = (victimIx + 1) % uint32(cf.bucketSize)
	}

	// If weren't able to insert, we roll back and try to insert new element in new filter.
	for i := 0; i < int(cf.maxIter); i++ {
		victimIx = (victimIx + uint32(cf.bucketSize) - 1) % uint32(cf.bucketSize)
		p = uint64(altHash(fp, cuckooHash(p))) % curFilter.bucketNum
		bucket := &curFilter.buckets[p]
		bucket.slots[victimIx], fp = fp, bucket.slots[victimIx]
	}

	return cuckooNospace
}

func (cf *CuckooFilter) insertFp(params params) cuckooInsertStatus {
	for i := int(cf.filterNum) - 1; i >= 0; i-- {
		if slot, ok := cf.filters[i].findAvailableSlot(params); ok {
			*slot = params.fp
			cf.itemNum++
			return cuckooInserted
		}
	}

	// No space, time to evict.
	if cf.evictAndInsert(params) == cuckooInserted {
		cf.itemNum++
		return cuckooInserted
	}

	if cf.expansion == 0 {
		return cuckooNospace
	}

	cf.grow()
	return cf.insertFp(params)
}

func (cf *CuckooFilter) Insert(data []byte) bool {
	status := cf.insertFp(buildParams(data))
	return status == cuckooInserted || status == cuckooAlreadyExist
}

func (cf *CuckooFilter) Delete(data []byte) bool {
	params := buildParams(data)
	for i := int(cf.filterNum) - 1; i >= 0; i-- {
		if cf.filters[i].delete(params) {
			cf.itemNum--
			cf.deleteNum++
			if cf.filterNum > 1 && float64(cf.deleteNum) > float64(cf.itemNum)*0.1 {
				cf.compact(false)
			}
			return true
		}
	}
	return false
}

func (cf *CuckooFilter) existFp(params params) bool {
	for i := range cf.filters {
		if cf.filters[i].find(params) {
			return true
		}
	}
	return false
}

func (cf *CuckooFilter) Exist(data []byte) bool {
	return cf.existFp(buildParams(data))
}

func (cf *CuckooFilter) Count(data []byte) uint64 {
	params := buildParams(data)
	res := uint64(0)
	for i := range cf.filters {
		res += uint64(cf.filters[i].count(params))
	}
	return res
}

const (
	relocEmpty = 0
	relocOk    = 1
	relocFail  = -1
)

// Attempt to move a fingerprint from one bucket to another filter.
func (cf *CuckooFilter) relocateSlot(bucket *bucket, filterIx uint16, bIx int, sIx int) int {
	if bucket.slots[sIx] == nullFp {
		return relocEmpty
	}

	params := params{
		h1: cuckooHash(bIx),
		fp: bucket.slots[sIx],
	}
	params.h2 = altHash(params.fp, params.h1)

	// Look at all the prior filters.
	for i := 0; i < int(filterIx); i++ {
		if slot, ok := cf.filters[i].findAvailableSlot(params); ok {
			*slot = params.fp
			bucket.slots[sIx] = nullFp
			return relocOk
		}
	}
	return relocFail
}

func (cf *CuckooFilter) compactSingle(filterIx uint16) int {
	curFilter := &cf.filters[filterIx]
	rv := relocOk

	for bIx := 0; bIx < int(curFilter.bucketNum); bIx++ {
		for sIx := 0; sIx < int(curFilter.bucketSize); sIx++ {
			if cf.relocateSlot(&curFilter.buckets[bIx], filterIx, bIx, sIx) == relocFail {
				rv = relocFail
			}
		}
	}

	// we free a filter only if it is the latest one
	if rv == relocOk && filterIx == cf.filterNum-1 {
		cf.filters = cf.filters[:cf.filterNum-1]
		cf.filterNum--
	}

	return rv
}

// Attempt to move elements to older filters. If latest filter is emptied, if is freed.
// `cont` determines whether to continue iteration on other filters once a filter cannot be freed
// and therefore following filter cannot be freed either.
func (cf *CuckooFilter) compact(cont bool) {
	for i := cf.filterNum - 1; i >= 1; i-- {
		if cf.compactSingle(i) == relocFail && !cont {
			break
		}
	}
	cf.deleteNum = 0
}
