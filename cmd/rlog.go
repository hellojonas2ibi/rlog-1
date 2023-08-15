package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/textproto"
	"os"
	"path/filepath"
	"time"
)

func main() {
	host := ""
	port := "9898"
	protocol := "tcp"
	address := fmt.Sprintf("%s:%s", host, port)
	server, err := net.Listen(protocol, address)
	log.Println("application started, listening on " + address)
	if err != nil {
		log.Fatalf("error starting server. %v\n", err)
	}
	defer server.Close()
	for {
		conn, err := server.Accept()
		if err != nil {
			log.Printf("error accepting connection. %v\n", err)
		}
		go handleConnection(conn)
	}
}

type entry struct {
	Group   string    `json:"group"`
	Time    time.Time `json:"time"`
	Level   string    `json:"level"`
	Message string    `json:"message"`
}

func handleConnection(conn net.Conn) {
	buffer := bytes.NewBuffer(make([]byte, 0))
	cData := make(chan bool)
	cResume := make(chan bool)
	go readData(conn, buffer, cData, cResume)
	go processData(buffer, cData, cResume)
}

func readData(conn net.Conn, buffer *bytes.Buffer, cData chan bool, cResume chan bool) {
	// TODO: stop this goroutine when client disconnects
	defer conn.Close()
	deadline := time.Now().Add(time.Duration(10) * time.Minute)
	if err := conn.SetDeadline(deadline); err != nil {
		log.Printf("[ERROR] failed setting read deadline. %v\n", err)
	}
	ticker := time.NewTicker(10 * time.Second)
	reader := bufio.NewReader(conn)
	tp := textproto.NewReader(reader)
	for {
		select {
		case <-ticker.C:
			cData <- true
			select {
			case <-cResume:
				buffer.Reset()
				continue
			}
		default:
			line, err := tp.ReadLineBytes()
			if err != nil && errors.Is(err, os.ErrDeadlineExceeded) {
				log.Printf("[ERROR] timeout, closed connection. %v\n", err)
				return
			} else if err == io.EOF {
				time.Sleep(time.Duration(10) * time.Second)
				continue
			}
			if len(line) == 0 {
				continue
			}
			line = append(line, byte('\n'))
			if _, err := buffer.Write(line); err != nil {
				log.Printf("[ERROR] failed writing chunk. %v\n", err)
			}
			deadline = time.Now().Add(time.Duration(10) * time.Minute)
			if err := conn.SetDeadline(deadline); err != nil {
				log.Printf("[ERROR] failed setting read deadline. %v\n", err)
			}
		}
	}
}

func processData(buffer *bytes.Buffer, cData chan bool, cResume chan bool) {
	// TODO: stop this goroutine when client disconnects
	entries := make(map[string][]byte)
	for {
		select {
		case <-cData:
			scanner := bufio.NewScanner(buffer)
			var e entry
			for scanner.Scan() {
				part := scanner.Bytes()
				if scanner.Err() != nil {
					break
				}
				if len(part) == 0 {
					continue
				}
				if err := json.Unmarshal(part, &e); err != nil {
					log.Printf("[ERROR] failed unmarshalling json data. %v\n", err)
					continue
				}
				part = append(part, byte('\n'))
				lines := entries[e.Group]
				entries[e.Group] = append(lines, part...)
			}
			if len(entries) > 0 {
				persistEntries(entries)
			}
			cResume <- true
		}
	}
}

func persistEntries(entryGroup map[string][]byte) {
	for group, entries := range entryGroup {
		log.Printf("persisiting %d bytes on group %s", len(entries), group)
		file, err := openLogFile(group)
		if err != nil {
			log.Printf("[ERROR] failed opening log file. %v\n", err)
			continue
		}
		defer file.Close()
		writer := bufio.NewWriter(file)
		writer.Write(entries)
		writer.Flush()
		delete(entryGroup, group)
	}
}

func openLogFile(group string) (*os.File, error) {
	date := time.Now()
	year := fmt.Sprint(date.Year())
	month := fmt.Sprint(int(date.Month()))
	day := fmt.Sprint(date.Day())
	filename := fmt.Sprintf("%s_%s-%s-%s.log", group, year, month, day)
	baseDir := "."
	logDir := filepath.Join(baseDir, year, month, day)
	logFile := filepath.Join(logDir, filename)
	file, err := os.OpenFile(logFile, os.O_WRONLY|os.O_CREATE|os.O_APPEND, os.ModePerm)
	if err == nil {
		return file, nil
	}
	stat, err := os.Stat(logDir)
	if (err != nil && os.IsNotExist(err)) || !stat.IsDir() {
		if err = os.MkdirAll(logDir, os.ModePerm); err != nil {
			return nil, err
		}
	}
	file, err = os.OpenFile(logFile, os.O_APPEND|os.O_CREATE|os.O_WRONLY, os.ModePerm)
	if err != nil {
		return nil, err
	}
	return file, nil
}
