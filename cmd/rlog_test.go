package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

func _TestReader(t *testing.T) {
	buf := bytes.NewBuffer(make([]byte, 0))
	b := []byte("line1\nline2\nline3")
	if _, err := buf.Write(b); err != nil {
		t.Fatalf("error writing log entry. %v\n", err)
	}
	reader := bufio.NewReader(buf)
	for reader.Buffered() > 0 {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		fmt.Printf("---> %s\n", string(line))
	}
	buf.Reset()
	b = []byte("line4\nline5\nline6")
	if _, err := buf.Write(b); err != nil {
		t.Fatalf("error writing log entry. %v\n", err)
	}
	for reader.Buffered() > 0 {
		line, _, err := reader.ReadLine()
		if err != nil {
			break
		}
		fmt.Printf("---> %s\n", string(line))
	}
}

func TestSendLogs(t *testing.T) {
	client, err := net.Dial("tcp", "localhost:9898")
	if err != nil {
		t.Fatalf("error dialing server. %v\n", err)
	}
	defer client.Close()
	groups := 30
	lines := 10000
	wg := sync.WaitGroup{}
	jsonData := make(chan []byte)
	go func() {
		for {
			select {
			case data := <-jsonData:
				client.Write(data)
			}
		}
	}()
	for i := 0; i < groups; i++ {
		wg.Add(1)
		go func(group int) {
			for j := 0; j < lines; j++ {
				e := entry{
					Group:   fmt.Sprintf("group%04d", group+1),
					Time:    time.Now(),
					Level:   "INFO",
					Message: fmt.Sprintf("Log entry number %04d", j+1),
				}
				bEntry, err := json.Marshal(e)
				if err != nil {
					continue
				}
				bEntry = append(bEntry, byte('\n'))
				jsonData <- bEntry
				time.Sleep(time.Millisecond * 50)
			}
			wg.Done()
		}(i)
	}
	wg.Wait()
	time.Sleep(time.Minute * 10)
}
