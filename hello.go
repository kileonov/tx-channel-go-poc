package main

import (
	"container/heap"
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"sync"
	"time"

	"github.com/google/uuid"
)

type Transaction struct {
	id      uuid.UUID
	instant time.Time
	amount  int
	qty     int
}

func (t *Transaction) mapToAccountingTransaction() AccountingTranasction {
	return AccountingTranasction{
		id:      t.id,
		instant: t.instant,
		amount:  t.amount,
		qty:     t.qty,
	}
}

type AccountingTranasction struct {
	id      uuid.UUID
	instant time.Time
	amount  int
	qty     int
}

type Lot struct {
	id     uuid.UUID
	amount int
	index  int // for heap implementation
}

type LotHeap []*Lot

func (h LotHeap) Len() int           { return len(h) }
func (h LotHeap) Less(i, j int) bool { return h[i].amount < h[j].amount }
func (h LotHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}

func (h *LotHeap) Push(x interface{}) {
	n := len(*h)
	lot := x.(*Lot)
	lot.index = n
	*h = append(*h, lot)
}

func (h *LotHeap) Pop() interface{} {
	old := *h
	n := len(old)
	lot := old[n-1]
	old[n-1] = nil // avoid memory leak
	lot.index = -1 // for safety
	*h = old[0 : n-1]
	return lot
}

func (at *AccountingTranasction) generateLots() []*Lot {
	result := make([]*Lot, 0, at.qty)
	for i := 0; i < at.qty; i++ {
		result = append(result, &Lot{
			id:     uuid.New(),
			amount: at.amount / at.qty,
		})
	}
	return result
}

func generateTransactions(ctx context.Context, limit int) <-chan Transaction {
	ch := make(chan Transaction, 10000)

	go func() {
		defer close(ch)
		for i := 0; i < limit; i++ {
			transaction := Transaction{
				id:      uuid.New(),
				instant: time.Now(),
				amount:  rand.Intn(1000),
				qty:     3,
			}

			select {
			case <-ctx.Done():
				return
			case ch <- transaction:
			}
		}
	}()

	return ch
}

func mapTransactionsToAccountingTransactions(ctx context.Context, transactionChannel <-chan Transaction, numWorkers int) <-chan AccountingTranasction {
	ch := make(chan AccountingTranasction, numWorkers*2) // Buffer size based on number of workers
	var wg sync.WaitGroup

	// Start worker pool
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for transaction := range transactionChannel {
				select {
				case <-ctx.Done():
					return
				default:
					accountingTranasction := transaction.mapToAccountingTransaction()
					select {
					case <-ctx.Done():
						return
					case ch <- accountingTranasction:
					}
				}
			}
		}()
	}

	// Close output channel when all workers are done
	go func() {
		wg.Wait()
		close(ch)
	}()

	return ch
}

func processLots(ctx context.Context, accountingChannel <-chan AccountingTranasction, numWorkers int) *LotHeap {
	lotHeap := &LotHeap{}
	heap.Init(lotHeap)
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Start worker pool
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for accountingTransaction := range accountingChannel {
				select {
				case <-ctx.Done():
					return
				default:
					lots := accountingTransaction.generateLots()
					mu.Lock()
					for _, lot := range lots {
						heap.Push(lotHeap, lot)
					}
					mu.Unlock()
				}
			}
		}()
	}

	// Wait for all workers to finish
	wg.Wait()
	return lotHeap
}

func main() {
	// Start time measurement
	startTime := time.Now()

	// Start memory measurement
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	startAlloc := m.TotalAlloc

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Determine number of workers based on CPU cores
	numWorkers := runtime.NumCPU()

	// Pre-allocate channels with reasonable buffer sizes
	transactionChannel := generateTransactions(ctx, 1_000_000)
	accountingChannel := mapTransactionsToAccountingTransactions(ctx, transactionChannel, numWorkers)
	lotHeap := processLots(ctx, accountingChannel, numWorkers)

	// Calculate and print performance metrics
	duration := time.Since(startTime)
	runtime.ReadMemStats(&m)
	memoryUsed := m.TotalAlloc - startAlloc

	totalLots := lotHeap.Len()
	totalTransactions := 1_000_000 // We know this from the input

	fmt.Printf("\nPerformance Metrics:\n")
	fmt.Printf("Total Duration: %v\n", duration)
	fmt.Printf("Memory Used: %v bytes\n", memoryUsed)
	fmt.Printf("Transactions Processed: %d\n", totalTransactions)
	fmt.Printf("Total Lots Generated: %d\n", totalLots)
	fmt.Printf("Average Lots per Transaction: %.2f\n", float64(totalLots)/float64(totalTransactions))
	fmt.Printf("Number of Workers: %d\n", numWorkers)
	fmt.Printf("Transactions per second: %.2f\n", float64(totalTransactions)/duration.Seconds())

	// Optional: Print some stats about the heap
	if totalLots > 0 {
		minLot := (*lotHeap)[0]
		fmt.Printf("\nHeap Statistics:\n")
		fmt.Printf("Minimum Lot Amount: %d\n", minLot.amount)
	}
}
