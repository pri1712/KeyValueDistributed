package utils

import (
	_ "fmt"
	"log"
	"runtime/debug"
)

func RecoverWithStackTrace(functionName string, serverID int) {
	if r := recover(); r != nil {
		log.Printf("PANIC in %s on server %d: %v", functionName, serverID, r)
		log.Printf("Stack trace:\n%s", debug.Stack())
		panic(r)
	}
}
