package kafkamock

import (
	"errors"
	"io"
	"net"
	"os"
	"syscall"
)

func wasSocketClosed(err error) bool {
	if errors.Is(err, io.EOF) || errors.Is(err, net.ErrClosed) {
		return true
	}

	opErr, _ := err.(*net.OpError)
	if opErr != nil {
		sysErr, _ := opErr.Err.(*os.SyscallError)
		if sysErr != nil {
			errno, _ := sysErr.Err.(syscall.Errno)
			if errno == syscall.ECONNRESET {
				return true
			}
		}

	}

	return false
}
