package disk

import (
	"io"
	"math"
	"sync"
	"time"
)

type SpeedReader struct {
	reader  io.Reader
	onCount func(uint64)
}

func NewSpeedReader(r io.Reader, onCount func(uint64)) *SpeedReader {
	return &SpeedReader{
		reader:  r,
		onCount: onCount,
	}
}

func (cr *SpeedReader) Read(p []byte) (n int, err error) {
	n, err = cr.reader.Read(p)
	cr.onCount(uint64(n))
	return n, err
}

type SpeedLimitedReader struct {
	exit chan struct{}

	reader io.Reader
	mu     sync.RWMutex

	targetBytesPerSec float64
	currentLimit      float64

	window     *SlidingWindow
	windowSize time.Duration

	learningRate float64
	momentum     float64
	lastGradient float64

	onCount      func(uint64)
	lastReadTime time.Time

	burstCapacity float64
	burstTokens   float64
	lastTokenFill time.Time

	smoothingFactor float64
	minChunkSize    int
}

type SlidingWindow struct {
	mu         sync.Mutex
	entries    []windowEntry
	head       int
	tail       int
	count      int
	maxAge     time.Duration
	totalBytes int64
}

type windowEntry struct {
	timestamp time.Time
	bytes     int
}

const DefaultLimitFactor = 1
const DefaultLearnRate = 0.1
const DefaultMomentum = 0.9
const DefaultBurstFactor = 0.01

func NewSpeedLimitedReader(r io.Reader, bytesPerSecond uint64, onCount func(uint64)) *SpeedLimitedReader {
	windowSize := 1 * time.Second

	slr := &SpeedLimitedReader{
		exit:              make(chan struct{}),
		reader:            r,
		targetBytesPerSec: float64(bytesPerSecond),
		currentLimit:      float64(bytesPerSecond) * DefaultLimitFactor,
		window:            NewSlidingWindow(windowSize),
		windowSize:        windowSize,
		learningRate:      DefaultLearnRate,
		momentum:          DefaultMomentum,
		lastGradient:      0,
		burstCapacity:     float64(bytesPerSecond) * DefaultBurstFactor,
		burstTokens:       float64(bytesPerSecond) * DefaultBurstFactor,
		lastTokenFill:     time.Now(),
		smoothingFactor:   0.95,
		minChunkSize:      32768,
		onCount:           onCount,
		lastReadTime:      time.Now(),
	}

	go slr.optimizationLoop()

	return slr
}

func (slr *SpeedLimitedReader) Close() {
	close(slr.exit)
}

func (slr *SpeedLimitedReader) Read(p []byte) (n int, err error) {
	start := time.Now()

	allowedBytes := slr.calculateAllowedBytes(len(p), start)

	if allowedBytes <= 0 {
		waitTime := slr.calculateWaitTime(len(p))
		if waitTime > 0 {
			time.Sleep(waitTime)
			allowedBytes = slr.calculateAllowedBytes(len(p), time.Now())
		}
	}

	readSize := minInt(len(p), allowedBytes)
	if readSize < slr.minChunkSize && len(p) >= slr.minChunkSize {
		waitTime := slr.calculateWaitTime(slr.minChunkSize)
		time.Sleep(waitTime)
		readSize = minInt(len(p), slr.calculateAllowedBytes(len(p), time.Now()))
	}

	n, err = slr.reader.Read(p[:readSize])

	if n > 0 {
		slr.window.Add(n, time.Now())
		slr.onCount(uint64(n))

		slr.mu.Lock()
		slr.lastReadTime = time.Now()
		slr.mu.Unlock()
	}

	return n, err
}

func (slr *SpeedLimitedReader) calculateAllowedBytes(requested int, now time.Time) int {
	slr.mu.Lock()
	defer slr.mu.Unlock()

	elapsed := now.Sub(slr.lastTokenFill).Seconds()
	if elapsed > 0 {
		tokensToAdd := slr.currentLimit * elapsed
		slr.burstTokens = math.Min(slr.burstTokens+tokensToAdd, slr.burstCapacity)
		slr.lastTokenFill = now
	}

	currentRate := slr.window.GetRate()

	if currentRate < slr.currentLimit {
		headroom := slr.currentLimit - currentRate
		allowed := int(math.Min(float64(requested), headroom+slr.burstTokens))

		if float64(allowed) > headroom {
			slr.burstTokens -= float64(allowed) - headroom
		}

		return allowed
	}

	if slr.burstTokens > 0 {
		allowed := int(math.Min(float64(requested), slr.burstTokens))
		slr.burstTokens -= float64(allowed)
		return allowed
	}

	return 0
}

func (slr *SpeedLimitedReader) calculateWaitTime(bytesNeeded int) time.Duration {
	slr.mu.RLock()
	defer slr.mu.RUnlock()

	currentRate := slr.window.GetRate()
	if currentRate >= slr.currentLimit {
		tokensNeeded := float64(bytesNeeded)
		timeToWait := tokensNeeded / slr.currentLimit
		return time.Duration(timeToWait * float64(time.Second))
	}

	return 0
}

func (slr *SpeedLimitedReader) optimizationLoop() {
	for {
		select {
		case <-time.After(500 * time.Millisecond):
			slr.optimizeLimit()
		case <-slr.exit:
			return
		}
	}
}

func (slr *SpeedLimitedReader) optimizeLimit() {
	slr.mu.Lock()
	defer slr.mu.Unlock()

	currentRate := slr.window.GetRate()
	errDiff := slr.targetBytesPerSec - currentRate

	gradient := errDiff / slr.targetBytesPerSec

	gradient = slr.momentum*slr.lastGradient + (1-slr.momentum)*gradient
	slr.lastGradient = gradient

	adjustment := slr.learningRate * gradient * slr.currentLimit

	slr.currentLimit = slr.smoothingFactor*slr.currentLimit + (1-slr.smoothingFactor)*(slr.currentLimit+adjustment)

	slr.currentLimit = math.Max(slr.targetBytesPerSec*0.8, math.Min(slr.targetBytesPerSec*1.2, slr.currentLimit))
}

func (slr *SpeedLimitedReader) Reset(bytesPerSec uint64) {
	slr.mu.Lock()
	defer slr.mu.Unlock()

	slr.targetBytesPerSec = float64(bytesPerSec)
	slr.currentLimit = float64(bytesPerSec) * DefaultLimitFactor
	slr.window = NewSlidingWindow(slr.windowSize)
	slr.learningRate = DefaultLearnRate
	slr.momentum = DefaultMomentum
	slr.lastGradient = 0
	slr.burstCapacity = float64(bytesPerSec) * DefaultBurstFactor
	slr.burstTokens = float64(bytesPerSec) * DefaultBurstFactor
	slr.lastTokenFill = time.Now()
}

const windowCapacity = 1000

func NewSlidingWindow(duration time.Duration) *SlidingWindow {
	return &SlidingWindow{
		entries: make([]windowEntry, windowCapacity),
		maxAge:  duration,
	}
}

func (sw *SlidingWindow) Add(bytes int, timestamp time.Time) {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	sw.cleanOldEntries(timestamp)

	if sw.count == len(sw.entries) {
		sw.totalBytes -= int64(sw.entries[sw.head].bytes)
		sw.head = (sw.head + 1) % len(sw.entries)
		sw.count--
	}

	sw.entries[sw.tail] = windowEntry{timestamp: timestamp, bytes: bytes}
	sw.tail = (sw.tail + 1) % len(sw.entries)
	sw.count++
	sw.totalBytes += int64(bytes)
}

func (sw *SlidingWindow) GetRate() float64 {
	sw.mu.Lock()
	defer sw.mu.Unlock()

	now := time.Now()
	sw.cleanOldEntries(now)

	if sw.count == 0 {
		return 0
	}

	oldest := sw.entries[sw.head].timestamp
	timeSpan := now.Sub(oldest).Seconds()
	if timeSpan <= 0 {
		return 0
	}

	return float64(sw.totalBytes) / timeSpan
}

func (sw *SlidingWindow) cleanOldEntries(now time.Time) {
	cutoff := now.Add(-sw.maxAge)
	for sw.count > 0 && sw.entries[sw.head].timestamp.Before(cutoff) {
		sw.totalBytes -= int64(sw.entries[sw.head].bytes)
		sw.head = (sw.head + 1) % len(sw.entries)
		sw.count--
	}
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}
