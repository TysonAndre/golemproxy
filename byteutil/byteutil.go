package byteutil

func IsExclusivelyDigits(key []byte) bool {
	for _, c := range key {
		if c > '9' || c < '0' {
			return false
		}
	}
	return true
}
