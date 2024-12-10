package concurrent

import (
	"strconv"
	"sync"
	"testing"
)

func BenchmarkMap(b *testing.B) {
	b.Run("Sequential Store", func(b *testing.B) {
		m := NewMap[string, int]()
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			m.Store(strconv.Itoa(i), i)
		}
	})

	b.Run("Sequential Load", func(b *testing.B) {
		m := NewMap[string, int]()
		for i := 0; i < b.N; i++ {
			m.Store(strconv.Itoa(i), i)
		}
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			m.Load(strconv.Itoa(i))
		}
	})

	b.Run("Concurrent Store", func(b *testing.B) {
		m := NewMap[string, int]()
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Store(strconv.Itoa(i), i)
				i++
			}
		})
	})

	b.Run("Concurrent Load", func(b *testing.B) {
		m := NewMap[string, int]()
		for i := 0; i < b.N; i++ {
			m.Store(strconv.Itoa(i), i)
		}
		b.ResetTimer()

		b.RunParallel(func(pb *testing.PB) {
			i := 0
			for pb.Next() {
				m.Load(strconv.Itoa(i % b.N))
				i++
			}
		})
	})

	b.Run("Concurrent Mixed", func(b *testing.B) {
		m := NewMap[string, int]()
		var wg sync.WaitGroup
		workers := 4
		opsPerWorker := b.N / workers

		b.ResetTimer()

		for i := 0; i < workers; i++ {
			wg.Add(1)
			go func(start int) {
				defer wg.Done()
				for j := start; j < start+opsPerWorker; j++ {
					key := strconv.Itoa(j)
					if j%2 == 0 {
						m.Store(key, j)
					} else {
						m.Load(key)
					}
				}
			}(i * opsPerWorker)
		}
		wg.Wait()
	})
}
