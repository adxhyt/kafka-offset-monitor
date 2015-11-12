package main

func BubbleSort(vector []int64) []int64 {
	for i := 0; i < len(vector); i++ {
		flag := true
		for j := 0; j < len(vector)-i-1; j++ {
			if vector[j] > vector[j+1] {
				vector[j], vector[j+1] = vector[j+1], vector[j]
				flag = false
			}
		}
		if flag {
			break
		}
	}
	return vector
}

func Median(vector []int64) int64 {
	vector = BubbleSort(vector)

	size := len(vector)

	mid := size / 2
	if size%2 == 0 {
		return (vector[mid-1] + vector[mid]) / 2
	} else {
		return vector[mid]
	}
}

func Abs(x int64) int64 {
	switch {
	case x < 0:
		return -x
	case x == 0:
		return 0 // return correctly abs(-0)
	}
	return x
}
