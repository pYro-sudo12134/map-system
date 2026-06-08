package main

import (
	"bufio"
	"context"
	"log"
	"os"
)

func main() {
	reader := bufio.NewReader(os.Stdin)
	
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err.Error() == "EOF" {
				return
			}
			log.Printf("Read error: %v", err)
			continue
		}
		
		line = []byte(trimSpace(string(line)))
		
		if len(line) == 0 {
			continue
		}
		
		result, err := Handle(context.Background(), line)
		if err != nil {
			log.Printf("Handle error: %v", err)
			os.Stdout.WriteString(`{"error":"` + err.Error() + `"}\n`)
			continue
		}
		
		os.Stdout.WriteString(result + "\n")
	}
}

func trimSpace(s string) string {
	for len(s) > 0 && (s[0] == ' ' || s[0] == '\t' || s[0] == '\n' || s[0] == '\r') {
		s = s[1:]
	}
	for len(s) > 0 && (s[len(s)-1] == ' ' || s[len(s)-1] == '\t' || s[len(s)-1] == '\n' || s[len(s)-1] == '\r') {
		s = s[:len(s)-1]
	}
	return s
}