package custom_map

import (
	"fmt"
	"hash/fnv"
)

// Robin Hood hash map implementation for better cache performance
type RobinHoodMap struct {
	entries []Entry
	size    int
	count   int
	mask    int // size - 1, for fast modulo when size is power of 2
}

type Entry struct {
	Key      string
	Value    interface{}
	Hash     uint32
	Distance int8 // Distance from ideal position
	Empty    bool
}

// NewRobinHoodMap creates a new Robin Hood hash map
func NewRobinHoodMap(initialSize int) *RobinHoodMap {
	// Ensure size is power of 2 for fast modulo
	size := 1
	for size < initialSize {
		size <<= 1
	}
	if size < 16 {
		size = 16
	}
	
	entries := make([]Entry, size)
	for i := range entries {
		entries[i].Empty = true
	}
	
	return &RobinHoodMap{
		entries: entries,
		size:    size,
		count:   0,
		mask:    size - 1,
	}
}

// fastHash uses a simple but fast hash function
func (rhm *RobinHoodMap) fastHash(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

// Put inserts or updates a key-value pair using Robin Hood hashing
func (rhm *RobinHoodMap) Put(key string, value interface{}) {
	if float64(rhm.count)/float64(rhm.size) > 0.75 {
		rhm.resize()
	}
	
	hash := rhm.fastHash(key)
	pos := int(hash) & rhm.mask
	distance := int8(0)
	
	entry := Entry{
		Key:      key,
		Value:    value,
		Hash:     hash,
		Distance: distance,
		Empty:    false,
	}
	
	for {
		existing := &rhm.entries[pos]
		
		if existing.Empty {
			// Found empty slot
			*existing = entry
			rhm.count++
			return
		}
		
		if existing.Hash == hash && existing.Key == key {
			// Update existing key
			existing.Value = value
			return
		}
		
		// Robin Hood: if our distance is greater than existing entry's distance,
		// swap and continue with the displaced entry
		if distance > existing.Distance {
			entry, *existing = *existing, entry
			distance = existing.Distance
		}
		
		pos = (pos + 1) & rhm.mask
		distance++
		
		// Prevent infinite loop (should not happen with proper resizing)
		if distance > 127 {
			rhm.resize()
			rhm.Put(key, value) // Retry after resize
			return
		}
	}
}

// Get retrieves a value by key
func (rhm *RobinHoodMap) Get(key string) (interface{}, bool) {
	hash := rhm.fastHash(key)
	pos := int(hash) & rhm.mask
	distance := int8(0)
	
	for {
		entry := &rhm.entries[pos]
		
		if entry.Empty || distance > entry.Distance {
			// Key not found
			return nil, false
		}
		
		if entry.Hash == hash && entry.Key == key {
			return entry.Value, true
		}
		
		pos = (pos + 1) & rhm.mask
		distance++
		
		if distance > 127 {
			break
		}
	}
	
	return nil, false
}

// Delete removes a key-value pair
func (rhm *RobinHoodMap) Delete(key string) bool {
	hash := rhm.fastHash(key)
	pos := int(hash) & rhm.mask
	distance := int8(0)
	
	for {
		entry := &rhm.entries[pos]
		
		if entry.Empty || distance > entry.Distance {
			return false // Key not found
		}
		
		if entry.Hash == hash && entry.Key == key {
			// Found the key to delete
			rhm.count--
			
			// Shift subsequent entries back to fill the gap
			for {
				nextPos := (pos + 1) & rhm.mask
				nextEntry := &rhm.entries[nextPos]
				
				if nextEntry.Empty || nextEntry.Distance == 0 {
					break
				}
				
				// Move the next entry back
				rhm.entries[pos] = *nextEntry
				rhm.entries[pos].Distance--
				pos = nextPos
			}
			
			// Mark the final position as empty
			rhm.entries[pos].Empty = true
			rhm.entries[pos].Key = ""
			rhm.entries[pos].Value = nil
			
			return true
		}
		
		pos = (pos + 1) & rhm.mask
		distance++
		
		if distance > 127 {
			break
		}
	}
	
	return false
}

// resize doubles the size and rehashes all elements
func (rhm *RobinHoodMap) resize() {
	oldEntries := rhm.entries
	
	rhm.size *= 2
	rhm.mask = rhm.size - 1
	rhm.entries = make([]Entry, rhm.size)
	rhm.count = 0
	
	for i := range rhm.entries {
		rhm.entries[i].Empty = true
	}
	
	// Rehash all existing entries
	for _, entry := range oldEntries {
		if !entry.Empty {
			rhm.Put(entry.Key, entry.Value)
		}
	}
}

// Size returns the number of elements
func (rhm *RobinHoodMap) Size() int {
	return rhm.count
}

// LoadFactor returns current load factor
func (rhm *RobinHoodMap) LoadFactor() float64 {
	return float64(rhm.count) / float64(rhm.size)
}

// Stats returns debugging information
func (rhm *RobinHoodMap) Stats() (int, int, float64, int8) {
	maxDistance := int8(0)
	totalDistance := 0
	
	for _, entry := range rhm.entries {
		if !entry.Empty {
			if entry.Distance > maxDistance {
				maxDistance = entry.Distance
			}
			totalDistance += int(entry.Distance)
		}
	}
	
	avgDistance := float64(0)
	if rhm.count > 0 {
		avgDistance = float64(totalDistance) / float64(rhm.count)
	}
	
	fmt.Printf("Count: %d, Size: %d, Load Factor: %.3f, Max Distance: %d, Avg Distance: %.2f\n",
		rhm.count, rhm.size, rhm.LoadFactor(), maxDistance, avgDistance)
	
	return rhm.count, rhm.size, rhm.LoadFactor(), maxDistance
}

// Keys returns all keys
func (rhm *RobinHoodMap) Keys() []string {
	keys := make([]string, 0, rhm.count)
	for _, entry := range rhm.entries {
		if !entry.Empty {
			keys = append(keys, entry.Key)
		}
	}
	return keys
}

// Values returns all values
func (rhm *RobinHoodMap) Values() []interface{} {
	values := make([]interface{}, 0, rhm.count)
	for _, entry := range rhm.entries {
		if !entry.Empty {
			values = append(values, entry.Value)
		}
	}
	return values
}

// Entries returns all key-value pairs as a custom struct
type RHKeyValuePair struct {
	Key   string
	Value interface{}
}

func (rhm *RobinHoodMap) Entries() []RHKeyValuePair {
	entries := make([]RHKeyValuePair, 0, rhm.count)
	for _, entry := range rhm.entries {
		if !entry.Empty {
			entries = append(entries, RHKeyValuePair{
				Key:   entry.Key,
				Value: entry.Value,
			})
		}
	}
	return entries
}

// ForEach iterates through all key-value pairs with a callback function
func (rhm *RobinHoodMap) ForEach(fn func(key string, value interface{})) {
	for _, entry := range rhm.entries {
		if !entry.Empty {
			fn(entry.Key, entry.Value)
		}
	}
}

// ForEachBreakable allows early termination by returning true from callback
func (rhm *RobinHoodMap) ForEachBreakable(fn func(key string, value interface{}) bool) {
	for _, entry := range rhm.entries {
		if !entry.Empty {
			if fn(entry.Key, entry.Value) {
				return // Early termination
			}
		}
	}
}

// Iterator provides a more Go-like iteration pattern
type RobinHoodIterator struct {
	rhm   *RobinHoodMap
	index int
}

func (rhm *RobinHoodMap) Iterator() *RobinHoodIterator {
	return &RobinHoodIterator{rhm: rhm, index: -1}
}

func (iter *RobinHoodIterator) Next() bool {
	iter.index++
	for iter.index < len(iter.rhm.entries) {
		if !iter.rhm.entries[iter.index].Empty {
			return true
		}
		iter.index++
	}
	return false
}

func (iter *RobinHoodIterator) Key() string {
	return iter.rhm.entries[iter.index].Key
}

func (iter *RobinHoodIterator) Value() interface{} {
	return iter.rhm.entries[iter.index].Value
}

