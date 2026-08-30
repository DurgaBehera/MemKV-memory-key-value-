package server

import (
	"bufio"
	"fmt"
	"log"
	"memkv/internal/protocol"
	"memkv/internal/store"
	"net"
	"strings"
	"sync"
	"time"
)

// TCPServer handles TCP connections for the MemKV store
type TCPServer struct {
	addr     string
	store    *store.Store
	listener net.Listener
	mu       sync.Mutex
	wg       sync.WaitGroup
	running  bool
}

// NewTCPServer creates a new TCP server instance
func NewTCPServer(addr string, store *store.Store) *TCPServer {
	return &TCPServer{
		addr:   addr,
		store:  store,
		running: false,
	}
}

// Start begins listening for incoming connections
func (s *TCPServer) Start() error {
	s.mu.Lock()
	if s.running {
		s.mu.Unlock()
		return fmt.Errorf("server is already running")
	}
	s.mu.Unlock()

	listener, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %w", s.addr, err)
	}
	s.listener = listener

	s.mu.Lock()
	s.running = true
	s.mu.Unlock()

	log.Printf("MemKV server listening on %s", s.addr)

	// Accept connections in a loop
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			// Check if we're shutting down
			s.mu.Lock()
			if !s.running {
				s.mu.Unlock()
				break // Exit accept loop when stopping
			}
			s.mu.Unlock()
			if err != nil {
				log.Printf("Error accepting connection: %v", err)
				continue
			}
		}

		// Handle each connection in a new goroutine
		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handleConnection(c)
		}(conn)
	}

	return nil
}

// Stop gracefully shuts down the server
func (s *TCPServer) Stop() error {
	s.mu.Lock()
	if !s.running {
		s.mu.Unlock()
		return nil // Already stopped
	}
	s.running = false
	s.mu.Unlock()

	// Stop accepting new connections
	if s.listener != nil {
		if err := s.listener.Close(); err != nil {
			return fmt.Errorf("error closing listener: %w", err)
		}
	}

	// Wait for all connections to finish
	done := make(chan struct{})
	go func() {
		s.wg.Wait()
		close(done)
	}()

	// Wait with timeout
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		return fmt.Errorf("timeout waiting for connections to close")
	}

	return nil
}

// handleConnection processes commands from a single client
func (s *TCPServer) handleConnection(conn net.Conn) {
	defer conn.Close()

	reader := bufio.NewReader(conn)
	writer := bufio.NewWriter(conn)

	for {
		// Read command line
		line, err := reader.ReadString('\n')
		if err != nil {
			// Client disconnected or error
			if err.Error() != "EOF" {
				log.Printf("Error reading from client: %v", err)
			}
			return
		}

		// Trim whitespace
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}

		// Parse command
		cmd, args, err := protocol.Parse(line)
		if err != nil {
			// Send error response
			fmt.Fprintf(writer, "(error) %s\n", err)
			writer.Flush()
			continue
		}

		// Execute command
		var response string
		switch strings.ToUpper(cmd) {
		case "PING":
			response = "PONG"
		case "QUIT":
			fmt.Fprintf(writer, "OK\n")
			writer.Flush()
			return
		case "SET":
			if len(args) < 2 {
				response = "(error) wrong number of arguments for 'set' command"
			} else {
				key := args[0]
				value := strings.Join(args[1:], " ")
				s.store.Set(key, value)
				response = "OK"
			}
		case "GET":
			if len(args) != 1 {
				response = "(error) wrong number of arguments for 'get' command"
			} else {
				if value, ok := s.store.Get(args[0]); ok {
					response = value
				} else {
					response = "(nil)"
				}
			}
		case "DEL":
			if len(args) != 1 {
				response = "(error) wrong number of arguments for 'del' command"
			} else {
				count := s.store.Delete(args[0])
				response = fmt.Sprintf("%d", count)
			}
		case "EXISTS":
			if len(args) != 1 {
				response = "(error) wrong number of arguments for 'exists' command"
			} else {
				count := s.store.Exists(args[0])
				response = fmt.Sprintf("%d", count)
			}
		case "INCR":
			if len(args) != 1 {
				response = "(error) wrong number of arguments for 'incr' command"
			} else {
				value, err := s.store.Increment(args[0])
				if err != nil {
					response = fmt.Sprintf("(error) %s", err)
				} else {
					response = fmt.Sprintf("%d", value)
				}
			}
		case "EXPIRE":
			if len(args) != 2 {
				response = "(error) wrong number of arguments for 'expire' command"
			} else {
				var seconds int64
				var err error
				if _, err = fmt.Sscanf(args[1], "%d", &seconds); err != nil {
					response = "(error) value is not an integer"
				} else {
					count := s.store.Expire(args[0], time.Duration(seconds)*time.Second)
					response = fmt.Sprintf("%d", count)
				}
			}
		case "TTL":
			if len(args) != 1 {
				response = "(error) wrong number of arguments for 'ttl' command"
			} else {
				ttl := s.store.TTL(args[0])
				response = fmt.Sprintf("%d", ttl)
			}
		default:
			response = fmt.Sprintf("(error) unknown command '%s'", cmd)
		}

		// Send response
		fmt.Fprintf(writer, "%s\n", response)
		writer.Flush()
	}
}