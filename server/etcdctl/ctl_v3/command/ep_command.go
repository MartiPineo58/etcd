package command

import (
	"context"
	"fmt"
	"sync"
	"time"

	"go.etcd.io/etcd/client/v3"
)

type epHealth struct {
	Ep     string `json:"endpoint"`
	Health bool   `json:"health"`
	Took   string `json:"took"`
	Error  string `json:"error,omitempty"`
}

func epHealthList(cf clientFlags, eps []string) {
	var wg sync.WaitGroup
	hch := make(chan epHealth, len(eps))
	for _, ep := range eps {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			
			sec := time.Now()
			cli, err := clientv3.New(clientv3.Config{
				Endpoints:   []string{endpoint},
				DialTimeout: cf.dialTimeout,
			})
			if err != nil {
				hch <- epHealth{Ep: endpoint, Health: false, Error: err.Error()}
				return
			}
			defer cli.Close()

			ctx, cancel := context.WithTimeout(context.Background(), cf.dialTimeout)
			_, err = cli.Get(ctx, "health")
			cancel()

			if err != nil {
				hch <- epHealth{Ep: endpoint, Health: false, Error: err.Error(), Took: time.Since(sec).String()}
				return
			}

			hch <- epHealth{Ep: endpoint, Health: true, Took: time.Since(sec).String()}
		}(ep)
	}
	wg.Wait()
	close(hch)

	// Process results and exit with non-zero code if any endpoint is unhealthy
}