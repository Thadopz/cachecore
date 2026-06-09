package singleflight

import (
	"fmt"
	"sync"
)

type Result struct {
	Val interface{}
	Err error
}

type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
	chs []chan Result
}

type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

func (g *Group) Do(key string, fn func() (interface{}, error)) (interface{}, error) {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(call)
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	v, err := fn()
	g.completeCall(key, c, v, err)
	return c.val, c.err
}

func (g *Group) DoChan(key string, fn func() (interface{}, error)) <-chan Result {
	ch := make(chan Result, 1)

	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		c.chs = append(c.chs, ch)
		g.mu.Unlock()
		return ch
	}

	c := new(call)
	c.wg.Add(1)
	c.chs = append(c.chs, ch)
	g.m[key] = c
	g.mu.Unlock()

	go func() {
		v, err := safeCall(fn)
		g.completeCall(key, c, v, err)
	}()
	return ch
}

func safeCall(fn func() (interface{}, error)) (val interface{}, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("singleflight panic: %v", r)
		}
	}()
	return fn()
}

func (g *Group) completeCall(key string, c *call, val interface{}, err error) {
	c.val, c.err = val, err
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	chs := append([]chan Result(nil), c.chs...)
	g.mu.Unlock()

	if len(chs) == 0 {
		return
	}
	res := Result{Val: val, Err: err}
	for _, ch := range chs {
		ch <- res
		close(ch)
	}
}
