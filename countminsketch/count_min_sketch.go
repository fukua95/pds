package countminsketch

import (
	"errors"
	"math"

	"github.com/aviddiviner/go-murmur"
)

type CMS struct {
	width   uint
	depth   uint
	counter uint
	cells   [][]uint
}

func New(overEst float64, prob float64) (*CMS, error) {
	width, depth := dimFromProb(overEst, prob)

	if width <= 0 || depth <= 0 {
		return nil, errors.New("invalid Parameter")
	}
	if width > math.MaxUint/depth {
		return nil, errors.New("parameter are too large")
	}

	cms := &CMS{
		width:   width,
		depth:   depth,
		counter: 0,
		cells:   make([][]uint, depth),
	}
	for i := range cms.cells {
		cms.cells[i] = make([]uint, width)
	}

	return cms, nil
}

func (cms *CMS) hash(data []byte, seed uint64) uint {
	return uint(murmur.MurmurHash64A(data, seed))
}

// Recommend width and depth for expected n different items,
// with probability of an error (prob) and over estimation error (overEst)
func dimFromProb(overEst float64, prob float64) (uint, uint) {
	if overEst <= 0 || overEst >= 1 || prob <= 0 || prob >= 1 {
		return 0, 0
	}
	width := uint(math.Ceil(2.0 / overEst))
	depth := uint(math.Ceil(math.Log10(prob) / math.Log10(0.5)))
	return width, depth
}

func (cms *CMS) IncrBy(data []byte, val uint) uint {
	minCount := uint(math.MaxUint)
	for i := range cms.cells {
		hash := cms.hash(data, uint64(i)) % cms.width

		cms.cells[i][hash] += val
		if cms.cells[i][hash] < val {
			cms.cells[i][hash] = math.MaxUint
		}

		minCount = min(minCount, cms.cells[i][hash])
	}
	cms.counter += val
	return minCount
}

// Return an estimate counter for item.
func (cms *CMS) Query(data []byte) uint {
	minCount := uint(math.MaxUint)
	for i := range cms.cells {
		hash := cms.hash(data, uint64(i)) % cms.width
		minCount = min(minCount, cms.cells[i][hash])
	}
	return minCount
}
