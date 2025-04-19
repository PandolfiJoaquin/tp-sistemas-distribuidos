package common

func Filter[T any](slice []T, predicate func(T) bool) []T {
	var result []T
	for _, item := range slice {
		if predicate(item) {
			result = append(result, item)
		}
	}
	return result
}

func First[T any](slice []T, predicate func(T) bool) *T {
	for _, item := range slice {
		if predicate(item) {
			return &item
		}
	}
	return nil
}

func Map[T, U any](slice []T, mapper func(T) U) []U {
	var result []U
	for _, item := range slice {
		result = append(result, mapper(item))
	}
	return result
}

func Sum[T uint32 | int32 | int | float64](slice []T) int {
	sum := 0
	for _, item := range slice {
		sum += int(item)
	}
	return sum
}

func Flatten[T any](slices [][]T) []T {
	var result []T
	for _, slice := range slices {
		result = append(result, slice...)
	}
	return result
}
