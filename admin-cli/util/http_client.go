package util

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-resty/resty/v2"
)

type Arguments struct {
	Name  string
	Value string
}

type Result struct {
	Resp string
	Err  error
}

type HTTPRequestFunc func(addr string, args Arguments) (string, error)

func BatchCallHTTP(nodes []*PegasusNode, request HTTPRequestFunc, args Arguments) map[string]*Result {
	results := make(map[string]*Result)

	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(len(nodes))
	for _, n := range nodes {
		go func(node *PegasusNode) {
			_, cancel := context.WithTimeout(context.Background(), time.Second*10)
			defer cancel()
			result, err := request(node.TCPAddr(), args)
			mu.Lock()
			if err != nil {
				results[node.CombinedAddr()] = &Result{Err: err}
			} else {
				results[node.CombinedAddr()] = &Result{Resp: result}
			}
			mu.Unlock()
			wg.Done()
		}(n)
	}
	wg.Wait()

	return results
}

func CallHTTPGet(url string) (string, error) {
	resp, err := resty.New().SetTimeout(time.Second * 10).R().Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to call \"%s\": %s", url, err)
	}
	if resp.StatusCode() != 200 {
		return "", fmt.Errorf("failed to call \"%s\": code=%d", url, resp.StatusCode())
	}
	return string(resp.Body()), nil
}
