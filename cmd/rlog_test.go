package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"net"
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
	groups := 1
	lines := 100
	for i := 0; i < groups; i++ {
		for j := 0; j < lines; j++ {
			e := entry{
				Group:   "group1",
				Time:    time.Now(),
				Level:   "INFO",
				Message: fmt.Sprintf("Log entry number %4d", j),
			}
			bEntry, err := json.Marshal(e)
			if err != nil {
				t.Fatalf("error marshalling log entry. %v\n", err)
			}
			bEntry = append(bEntry, byte('\n'))
			if _, err = client.Write(bEntry); err != nil {
				t.Fatalf("error writing log entry. %v\n", err)
			}
		}
	}
}
