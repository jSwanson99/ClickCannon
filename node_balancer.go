package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"log"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
)

type NodeBalancer struct {
	chConfig *ClickHouseConfig

	nodeIPs []string
	index   int
	mu      sync.Mutex
}

func newNodeBalancer(chConfig *ClickHouseConfig) *NodeBalancer {
	return &NodeBalancer{
		chConfig: chConfig,
	}
}

func (b *NodeBalancer) FetchNodes() error {
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
	defer conn.Close()
	log.Println("[Node Balancer] clickhouse connected")

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
		log.Println("[Node Balancer] discovered IP:", nodeIP)
	}

	log.Printf("[Node Balancer] discovered %d IP(s)\n", len(b.nodeIPs))

	return nil
}

func (b *NodeBalancer) IsNextNode(ip string) bool {
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
