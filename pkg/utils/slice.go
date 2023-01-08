package utils

func Reverse(data []byte) []byte {
	n := len(data)
	res := make([]byte, n)
	for i := 0; i < n; i++ {
		res[i] = data[n-1-i]
	}

	return res
}
