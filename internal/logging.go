package internal

import (
	"log"
	"os"
)

var ErrLogger *log.Logger = log.New(os.Stderr, " [ERROR] ", log.Ldate|log.Ltime|log.Lshortfile)
var InfoLogger *log.Logger = log.New(os.Stdout, " [INFO] ", log.Ldate|log.Ltime|log.Lshortfile)
