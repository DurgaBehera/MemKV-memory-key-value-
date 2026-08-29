package protocol

import (
	"errors"
	"strings"
)

// Parse converts a command line into command name and arguments
func Parse(input string) (string, []string, error) {
	// Split by whitespace but preserve quoted strings for values
	parts := strings.Fields(input)
	if len(parts) == 0 {
		return "", nil, errors.New("empty command")
	}

	command := strings.ToUpper(parts[0])
	args := parts[1:]

	// Validate command
	switch command {
	case "PING", "QUIT":
		if len(args) != 0 {
			return "", nil, errors.New("wrong number of arguments")
		}
	case "SET":
		if len(args) < 2 {
			return "", nil, errors.New("wrong number of arguments for 'set' command")
		}
	case "GET", "DEL", "EXISTS", "INCR", "EXPIRE", "TTL":
		if len(args) != 1 {
			return "", nil, errors.New("wrong number of arguments for '" + strings.ToLower(command) + "' command")
		}
	default:
		return "", nil, errors.New("unknown command")
	}

	return command, args, nil
}