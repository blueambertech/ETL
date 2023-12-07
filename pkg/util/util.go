package util

// ToSlice converts a channel to a slice
func ToSlice[T any](ch chan T) []T {
	ts := make([]T, 0)
	for t := range ch {
		ts = append(ts, t)
	}
	return ts
}

// JoinChannelErrors takes a channel of errors and iterates over them to extract the error message, it then appends this
// to a string using the supplied seperator
func JoinChannelErrors(eChan chan error, sep string) string {
	errMsgs := ""
	for e := range eChan {
		if e != nil {
			errMsgs = errMsgs + e.Error() + sep
		}
	}
	return errMsgs
}

// RemoveZeroEntries takes a slice of comparable objects and iterates over them checking to see if they match their zero value,
// if they contain a non-zero value they are appended to a new slice which is returned
func RemoveZeroEntries[T comparable](a []T) []T {
	b := []T{}
	for _, o := range a {
		if o != *new(T) {
			b = append(b, o)
		}
	}
	return b
}
