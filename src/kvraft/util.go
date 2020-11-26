package kvraft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func FPrintf(format string, a ...interface{}) {
	log.Fatalf(format, a...)
}