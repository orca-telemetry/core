package dag

import (
	"fmt"
	"slices"
	"sort"
	"strconv"
	"strings"

	"gonum.org/v1/gonum/graph"
	"gonum.org/v1/gonum/graph/simple"
)

// Node represents an algorithm in the DAG
type Node struct {
	id         int64
	algoId     int64
	procId     int64
	windowId   int64
	algoDepIds []int64
	pathIdx    int
}

// ID satisfies the graph.Node interface.
func (n Node) ID() int64 {
	return n.id
}

func (n Node) AlgoId() int64 {
	return n.algoId
}

func (n Node) AlgoDepIds() []int64 {
	return n.algoDepIds
}

// ProcessorTask represents a set of tasks (nodes) assigned to a single processor
type ProcessorTask struct {
	ProcId int64
	Nodes  []Node
}

// Stage represents a set of processor tasks that can be executed in parallel
type Stage struct {
	Tasks []ProcessorTask
}

// Plan represents the full execution plan: a sequence of stages
type Plan struct {
	Stages             []Stage
	AffectedProcessors []int64
}

// LayeredTopoSort returns the nodes of the directed graph g grouped into
// layers, where each layer contains nodes that can be processed in parallel
func LayeredTopoSort(g graph.Directed) ([][]graph.Node, error) {
	// calculate in-degrees
	inDegree := make(map[int64]int)
	nodes := g.Nodes()
	for nodes.Next() {
		node := nodes.Node()
		neighbors := g.From(node.ID())
		for neighbors.Next() {
			neighbor := neighbors.Node()
			inDegree[neighbor.ID()]++
		}
	}

	// find initial nodes (in-degree == 0)
	var currentLevel []graph.Node
	nodes = g.Nodes()
	for nodes.Next() {
		node := nodes.Node()
		if inDegree[node.ID()] == 0 {
			currentLevel = append(currentLevel, node)
		}
	}

	var layers [][]graph.Node
	processedCount := 0

	for len(currentLevel) > 0 {
		layers = append(layers, currentLevel)

		var nextLevel []graph.Node
		for _, node := range currentLevel {
			processedCount++
			neighbors := g.From(node.ID())
			for neighbors.Next() {
				neighbor := neighbors.Node()
				inDegree[neighbor.ID()]--
				if inDegree[neighbor.ID()] == 0 {
					nextLevel = append(nextLevel, neighbor)
				}
			}
		}
		currentLevel = nextLevel
	}

	if processedCount != g.Nodes().Len() {
		return nil, fmt.Errorf("cycle detected in graph: topological layering not possible")
	}

	return layers, nil
}

// BuildPlan builds a parallel execution Plan from the DAG represented by algoExecPaths,
// windowExecPaths, and procExecPaths.
func BuildPlan(
	algoExecPaths []string,
	windowExecPaths []string,
	procExecPaths []string,
	targetWindowId int64,
) (Plan, error) {
	if len(algoExecPaths) != len(windowExecPaths) || len(windowExecPaths) != len(procExecPaths) {
		return Plan{}, fmt.Errorf(
			"number of graph paths do not match: algo=%d, window=%d, proc=%d",
			len(algoExecPaths),
			len(windowExecPaths),
			len(procExecPaths),
		)
	}

	g := simple.NewDirectedGraph()
	nodeMap := make(map[int64]Node) // map of algoIDs to nodes
	var nextId int64 = 1

	for pathIdx, algoPath := range algoExecPaths {
		algoSegments := splitPath(algoPath)
		procSegments := splitPath(procExecPaths[pathIdx])
		windowSegments := splitPath(windowExecPaths[pathIdx])

		if len(algoSegments) != len(windowSegments) ||
			len(windowSegments) != len(procSegments) {
			return Plan{}, fmt.Errorf(
				"number of processor segments do not match: algo=%d, window=%d, proc=%d",
				len(algoSegments),
				len(windowSegments),
				len(procSegments),
			)
		}

		var pathWindowMap map[int64]int64 // <<<<< added here
		var prevNode Node

		for ii, algoIdStr := range algoSegments {
			algoId := mustAtoi(algoIdStr)
			procId := mustAtoi(procSegments[ii])
			windowId := mustAtoi(windowSegments[ii])
			if pathWindowMap == nil {
				pathWindowMap = make(map[int64]int64)
			}

			if prevWin, seen := pathWindowMap[int64(procId)]; seen {
				if prevWin != int64(windowId) {
					return Plan{}, fmt.Errorf(
						"window ID mismatch on processor %d in path %d: saw %d, then %d",
						procId, pathIdx, prevWin, windowId,
					)
				}
			} else {
				pathWindowMap[int64(procId)] = int64(windowId)
			}

			node, exists := nodeMap[int64(algoId)]
			if !exists {
				node = Node{
					id:       nextId,
					algoId:   int64(algoId),
					procId:   int64(procId),
					windowId: int64(windowId),
					pathIdx:  pathIdx,
				}
				nodeMap[int64(algoId)] = node
				g.AddNode(node)
				nextId++
			}

			if prevNode.id != 0 {
				edge := g.NewEdge(prevNode, node)
				g.SetEdge(edge)
			}
			prevNode = node
		}
	}

	layers, err := LayeredTopoSort(g)
	if err != nil {
		return Plan{}, fmt.Errorf("error during layered topological sort: %v", err)
	}

	var plan Plan

	for _, layer := range layers {
		taskMap := make(map[int64][]Node)

		for _, gn := range layer {
			node := gn.(Node)

			// modify the node with the nodes dependencies
			nodes := g.To(node.ID())
			for range nodes.Len() {
				nodes.Next()
				_currNode := nodes.Node()
				_currNode_v2, ok := _currNode.(Node)
				if !ok {
					panic(ok)
				}
				if node.algoDepIds == nil {
					node.algoDepIds = []int64{_currNode_v2.algoId}
				} else {
					node.algoDepIds = append(node.algoDepIds, _currNode_v2.algoId)
				}
			}
			// sort the algo deps within the node
			slices.Sort(node.algoDepIds)

			taskMap[node.procId] = append(taskMap[node.procId], node)
		}
		var stage Stage
		for procId, nodes := range taskMap {
			if !slices.Contains(plan.AffectedProcessors, procId) {
				plan.AffectedProcessors = append(plan.AffectedProcessors, procId)
			}
			// sort nodes inside processor task
			sort.Slice(nodes, func(i, j int) bool {
				return nodes[i].pathIdx < nodes[j].pathIdx
			})

			stage.Tasks = append(stage.Tasks, ProcessorTask{
				ProcId: procId,
				Nodes:  nodes,
			})
		}
		// sort the processors inside the tasks
		sort.Slice(stage.Tasks, func(i, j int) bool {
			return stage.Tasks[i].ProcId < stage.Tasks[j].ProcId
		})
		slices.Sort(plan.AffectedProcessors)

		plan.Stages = append(plan.Stages, stage)
	}

	return plan, nil
}

// splitPath splits a path string into segments.
func splitPath(path string) []string {
	return strings.Split(path, ".")
}

// mustAtoi converts a string to an int, panicking if invalid.
func mustAtoi(s string) int {
	n, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Sprintf("invalid integer: %s", s))
	}
	return n
}
