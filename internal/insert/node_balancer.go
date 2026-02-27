package insert

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type nodeBalancer struct {
	log *slog.Logger

	chConfig *ClickHouseConfig

	nodeIPs []string
	index   int
	mu      sync.Mutex
}

func newNodeBalancer(log *slog.Logger, chConfig *ClickHouseConfig) *nodeBalancer {
	return &nodeBalancer{
		log:      log.With("component", "node_balancer"),
		chConfig: chConfig,
	}
}

func (b *nodeBalancer) FetchNodes() error {
	opt := clickhouse.Options{
		Addr: []string{b.chConfig.Address},
		Auth: clickhouse.Auth{
			Username: b.chConfig.User,
			Password: b.chConfig.Password,
		},
	}
	if b.chConfig.Secure {
		opt.TLS = &tls.Config{}
	}

	conn, err := clickhouse.Open(&opt)
	if err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer func(conn driver.Conn) {
		closeErr := conn.Close()
		if closeErr != nil {
			b.log.Error("failed to close clickhouse conn", "err", closeErr)
		}
	}(conn)
	b.log.Info("clickhouse connected")

	sql := `SELECT host_address FROM system.clusters WHERE cluster='default'`
	rows, err := conn.Query(context.Background(), sql)
	if err != nil {
		return fmt.Errorf("failed to query node IPs: %w", err)
	}

	for rows.Next() {
		var nodeIP string
		err = rows.Scan(&nodeIP)
		if err != nil {
			return fmt.Errorf("failed to scan nodeIP: %w", err)
		}

		b.nodeIPs = append(b.nodeIPs, nodeIP)
		b.log.Debug("discovered IP", "ip", nodeIP)
	}

	b.log.Info("discovered IP(s)", "count", len(b.nodeIPs))

	return nil
}

func (b *nodeBalancer) IsNextNode(ip string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()

	if ip != b.nodeIPs[b.index] {
		return false
	}

	b.index++
	if b.index == len(b.nodeIPs) {
		b.index = 0
	}

	return true
}
