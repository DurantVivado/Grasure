package grasure

func isConflict(arr1, arr2 *[]int) bool {
	for i := range *arr1 {
		for j := range *arr2 {
			if (*arr1)[i] == (*arr2)[j] {
				return true
			}
		}
	}
	return false
}
func (e *Erasure) graph_generator(fi *fileInfo, failStripeSet *[]IntSet) map[int][]int {
	dist := fi.Distribution
	graph := make(map[int][]int)
	for s1 := range *failStripeSet {
		for s2 := range *failStripeSet {
			if s1 < s2 && isConflict(&dist[s1], &dist[s2]) {
				graph[s1] = append(graph[s1], s2)
				graph[s2] = append(graph[s2], s1)
			}
		}
	}
	return graph
}
