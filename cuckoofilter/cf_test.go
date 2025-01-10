package cuckoofilter

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

const defaultBucketSize = 2

func TestBasicOps(t *testing.T) {
	cf := New(50, defaultBucketSize, 20, 1)
	assert.Equal(t, cf.itemNum, uint64(0))
	assert.Equal(t, cf.filterNum, uint16(1))

	k1 := []byte("key111")
	k2 := []byte("key222")
	k3 := []byte("key333")

	assert.True(t, cf.Insert(k1))
	assert.True(t, cf.Insert(k2))
	assert.True(t, cf.Exist(k1))
	assert.True(t, cf.Exist(k2))
	assert.False(t, cf.Exist(k3))
	assert.Equal(t, cf.itemNum, uint64(2))
	assert.True(t, cf.Insert(k3))
	assert.Equal(t, cf.itemNum, uint64(3))

	assert.True(t, cf.Delete(k1))
	assert.Equal(t, cf.itemNum, uint64(2))
	assert.False(t, cf.Exist(k1))
	assert.False(t, cf.Delete(k1))
}

func TestCount(t *testing.T) {
	cf := New(10, defaultBucketSize, 20, 1)
	k1 := []byte("key11111")
	assert.Equal(t, cf.Count(k1), uint64(0))

	assert.True(t, cf.Insert(k1))
	assert.Equal(t, cf.Count(k1), uint64(1))
	assert.True(t, cf.Insert(k1))
	assert.Equal(t, cf.Count(k1), uint64(2))

	for i := 0; i < 8; i++ {
		assert.True(t, cf.Insert(k1))
		assert.Equal(t, cf.Count(k1), uint64(3+i))
	}
	assert.Equal(t, cf.itemNum, uint64(10))
}

func TestRelocations(t *testing.T) {
	cap := 10000
	cf := New(uint64(cap/2), 4, 20, 1)
	assert.Equal(t, cf.itemNum, uint64(0))
	assert.Equal(t, cf.filterNum, uint16(1))

	for i := 0; i < cap; i++ {
		k := []byte(strconv.Itoa(i))
		assert.True(t, cf.Insert(k))
		for j := 0; j < i; j++ {
			k2 := []byte(strconv.Itoa(j))
			assert.True(t, cf.Exist(k2))
		}
	}
}

func fill(cf *CuckooFilter, cap int) {
	for i := 0; i < cap; i++ {
		k := []byte(strconv.Itoa(i))
		cf.Insert(k)
	}
}

func countCollision(t *testing.T, cf *CuckooFilter, cap int) uint {
	res := uint(0)
	for i := 0; i < cap; i++ {
		k := []byte(strconv.Itoa(i))
		count := cf.Count(k)
		assert.NotEqual(t, cf.Count(k), 0)
		if count > 1 {
			res++
		}
	}
	return res
}

func TestFalsePositiveRate(t *testing.T) {
	cap := 10000
	cf := New(uint64(cap), defaultBucketSize, 50, 1)
	assert.Equal(t, cf.bucketNum*uint64(cf.bucketSize), uint64(16384))

	fill(cf, cap)
	assert.Equal(t, cf.filterNum, uint16(1))
	assert.Equal(t, cf.itemNum, uint64(cap))

	fpr := 0.015 // 2 * defaultBucketSize / 256
	assert.LessOrEqual(t, float64(countCollision(t, cf, cap)), float64(cap)*fpr)

	for _, v := range []int{2, 4} {
		cf = New(uint64(cap/v), defaultBucketSize, 50, 1)
		fill(cf, cap)
		assert.Equal(t, cf.itemNum, uint64(cap))
		assert.LessOrEqual(t, float64(countCollision(t, cf, cap)), float64(cap)*fpr*float64(v))
	}
}

func TestDelete(t *testing.T) {
	cap := 10000
	cf := New(uint64(cap/8), defaultBucketSize, 50, 1)
	fill(cf, cap)
	assert.Equal(t, cf.itemNum, uint64(cap))
	for i := 0; i < cap; i++ {
		k := []byte(strconv.Itoa(i))
		assert.True(t, cf.Delete(k))
	}
	assert.Equal(t, cf.itemNum, uint64(0))
}

func TestDeleteWithExpansion(t *testing.T) {
	cap := 10000
	cf := New(uint64(cap/8), defaultBucketSize, 50, 2)
	fill(cf, cap)
	for i := 0; i < cap; i++ {
		k := []byte(strconv.Itoa(i))
		assert.True(t, cf.Delete(k))
	}
	assert.Equal(t, cf.itemNum, uint64(0))
}

func TestBucketSize(t *testing.T) {
	cap := 10000
	bucketSize := []uint16{1, 2, 4}
	ExpectedFilterNum := []uint16{12, 11, 10}
	for i := range bucketSize {
		cf := New(uint64(cap/10), bucketSize[i], 50, 1)
		fill(cf, cap)
		assert.Equal(t, cf.bucketSize, bucketSize[i])
		assert.Equal(t, cf.filterNum, ExpectedFilterNum[i])
	}
}
