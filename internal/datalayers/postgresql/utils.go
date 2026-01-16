package postgresql

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/orc-analytics/core/internal/dag"

	"github.com/orc-analytics/core/internal/envs"
	pb "github.com/orc-analytics/core/protobufs/go"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

func processTasks(
	d *Datalayer,
	executionPlan dag.Plan,
	window *pb.Window,
	insertedWindow RegisterWindowRow,
) error {
	ctx := context.Background()
	slog.Info("calculated execution paths", "execution_paths", executionPlan)
	// get map of processors from processor ids
	processorMap := make(
		map[int64]Processor,
		len(executionPlan.AffectedProcessors),
	)
	processors, err := d.queries.ReadProcessorsByIDs(ctx, executionPlan.AffectedProcessors)
	if err != nil {
		slog.Error("Processors could not be read", "error", err)
		return err
	}

	for _, proc := range processors {
		processorMap[proc.ID] = proc
	}

	// get map of algorithms from algorithm ids
	algorithmMap := make(
		map[int64]Algorithm,
	)

	// map of algorithm Ids to results
	resultMap := make(
		map[int64]*pb.ExecutionResult,
	)

	// map of execution IDs and the algorithms requested
	algorithms, err := d.queries.ReadAlgorithmsForWindow(ctx, ReadAlgorithmsForWindowParams{
		WindowTypeName:    window.WindowTypeName,
		WindowTypeVersion: window.WindowTypeVersion,
	})

	for _, algo := range algorithms {
		algorithmMap[algo.ID] = algo
	}
	// get the environment
	config := envs.GetConfig()

	// for each stage, build processsings
	slog.Debug("execution plan", "executionPlan", executionPlan)
	for _, stage := range executionPlan.Stages {
		for _, task := range stage.Tasks {
			proc, ok := processorMap[task.ProcId]
			if !ok {
				slog.Error("Processor not found for task", "proc_id", task.ProcId)
				return fmt.Errorf("processor ID %d not found", task.ProcId)
			}
			var conn *grpc.ClientConn

			if config.IsProduction {
				host, _, err := net.SplitHostPort(proc.ConnectionString)
				if err != nil {
					host = proc.ConnectionString
				}
				conn, err = grpc.NewClient(
					proc.ConnectionString,
					grpc.WithTransportCredentials(
						credentials.NewTLS(
							&tls.Config{
								ServerName: host,
							},
						),
					),
				)
			} else {
				conn, err = grpc.NewClient(
					proc.ConnectionString,
					grpc.WithTransportCredentials(insecure.NewCredentials()),
				)
			}
			if err != nil {
				slog.Error("could not connect to processor", "proc_id", task.ProcId, "error", err)
				return fmt.Errorf("could not contact processor: %w", err)
			}
			// WARN: close conn when done (not deferred inside a loop)
			defer func(conn *grpc.ClientConn) {
				if err := conn.Close(); err != nil {
					slog.Warn("error closing gRPC connection", "error", err)
				}
			}(conn)

			client := pb.NewOrcaProcessorClient(conn)
			healthCheckResponse, err := client.HealthCheck(ctx, &pb.HealthCheckRequest{
				Timestamp: time.Now().Unix(),
			})
			if err != nil {
				slog.Error(
					"issue contacting processor",
					"response",
					healthCheckResponse,
					"processor",
					proc,
				)

				return fmt.Errorf("issue contacting processor: %w", err)
			}
			if healthCheckResponse.Status != pb.HealthCheckResponse_STATUS_SERVING {
				slog.Error(
					"cannot execute stage, processor not serving",
					"status",
					healthCheckResponse.Status,
					"message",
					healthCheckResponse.Message,
				)
				return fmt.Errorf("cannot execute stage, processor not serving: %w", err)
			}

			// build list of affected Algorithms
			var affectedAlgorithms []*pb.Algorithm

			// generate an execution id
			execUuid := uuid.New()
			execId := strings.ReplaceAll(execUuid.String(), "-", "")

			algorithmExecutions := make([]*pb.ExecuteAlgorithm, len(task.Nodes))

			for ii, node := range task.Nodes {
				algo, ok := algorithmMap[node.AlgoId()]

				if !ok {
					slog.Error("algorithm not found", "algo_id", node.AlgoId())
					return fmt.Errorf("algorithm ID %d not found", node.AlgoId())
				}

				affectedAlgorithms = append(affectedAlgorithms, &pb.Algorithm{
					Name:    algo.Name,
					Version: algo.Version,
				})

				algorithm_dependencies := make([]*pb.AlgorithmDependencyResult, node.LenAlgoDeps())

				// determine which results need to be included
				jj := 0
				for algoDep := range node.AlgoDeps() {
					// get details of the algorithm - dependencies will only
					// exist in this block if they have run
					algorithm_result := resultMap[algoDep.AlgoId].GetAlgorithmResult()

					// log the result as the first entry before considering lookbacks
					dep_results := []*pb.AlgorithmDependencyResultRow{
						{
							Result: algorithm_result.GetResult(),
							Window: algorithm_result.GetWindow(),
						},
					}

					// handle algorithm lookbacks
					if algoDep.Lookback.Count > 0 {
						results, err := d.queries.ReadResultsForAlgorithmByCount(ctx, ReadResultsForAlgorithmByCountParams{
							AlgorithmID: pgtype.Int8{Int64: algoDep.AlgoId, Valid: true},
							Count:       int32(algoDep.Lookback.Count),
							SearchTo: pgtype.Timestamp{
								Time:  algorithm_result.GetWindow().GetTimeTo().AsTime().UTC(),
								Valid: true,
							},
						})

						if err != nil {
							return fmt.Errorf("could not read algorithm results with lookback count %d: %w", algoDep.Lookback.Count, err)

						}

						for _, res := range results {
							if algorithm_result.GetAlgorithm().GetResultType() == pb.ResultType_ARRAY {
								dep_results = append(dep_results, &pb.AlgorithmDependencyResultRow{
									Result: &pb.Result{ResultData: &pb.Result_FloatValues{
										FloatValues: &pb.FloatArray{Values: convertFloat64ToFloat32(res.ResultArray)},
									}},
								})
							} else if algorithm_result.GetAlgorithm().GetResultType() == pb.ResultType_STRUCT {
								var data map[string]any
								err := json.Unmarshal(res.ResultJson, &data)
								if err != nil {
									return err
								}

								result_Struct, err := structpb.NewStruct(data)
								if err != nil {
									return err
								}

								dep_results = append(dep_results, &pb.AlgorithmDependencyResultRow{
									Result: &pb.Result{ResultData: &pb.Result_StructValue{
										StructValue: result_Struct,
									}},
								})
							} else if algorithm_result.GetAlgorithm().GetResultType() == pb.ResultType_VALUE {
								dep_results = append(dep_results, &pb.AlgorithmDependencyResultRow{
									Result: &pb.Result{ResultData: &pb.Result_SingleValue{
										SingleValue: algorithm_result.GetResult().GetSingleValue(),
									}},
								})
							}
						}
						algorithm_dependencies[jj] = &pb.AlgorithmDependencyResult{
							Algorithm: algorithm_result.GetAlgorithm(),
							Result:    dep_results,
						}

					} else if algoDep.Lookback.Timedelta > 0 {
						earliest_time_of_latest_result := algorithm_result.GetWindow().GetTimeFrom().AsTime().UTC()
						search_from := earliest_time_of_latest_result.Add(-time.Duration(algoDep.Lookback.Timedelta))

						results, err := d.queries.ReadResultsForAlgorithmByTimedelta(ctx, ReadResultsForAlgorithmByTimedeltaParams{
							AlgorithmID: pgtype.Int8{Int64: algoDep.AlgoId, Valid: true},
							SearchFrom: pgtype.Timestamp{
								Time:  search_from,
								Valid: true,
							},
							SearchTo: pgtype.Timestamp{
								Time:  algorithm_result.GetWindow().GetTimeFrom().AsTime().UTC(),
								Valid: true,
							},
						})

						if err != nil {
							return fmt.Errorf("could not read algorithm results with lookback count %d: %w", algoDep.Lookback.Count, err)

						}

						for _, res := range results {
							if algorithm_result.GetAlgorithm().GetResultType() == pb.ResultType_ARRAY {
								dep_results = append(dep_results, &pb.AlgorithmDependencyResultRow{
									Result: &pb.Result{ResultData: &pb.Result_FloatValues{
										FloatValues: &pb.FloatArray{Values: convertFloat64ToFloat32(res.ResultArray)},
									}},
								})
							}
						}
						algorithm_dependencies[jj] = &pb.AlgorithmDependencyResult{
							Algorithm: algorithm_result.GetAlgorithm(),
							Result:    dep_results,
						}
						// TODO:
					}
					jj++
				}
				algorithmExecutions[ii] = &pb.ExecuteAlgorithm{
					Algorithm: &pb.Algorithm{
						Name:    algo.Name,
						Version: algo.Version,
					},
					Dependencies: algorithm_dependencies,
				}
			}

			execReq := &pb.ExecutionRequest{
				ExecId:              execId,
				Window:              window,
				AlgorithmExecutions: algorithmExecutions,
			}

			stream, err := client.ExecuteDagPart(ctx, execReq)
			if err != nil {
				slog.Error(
					"failed to start DAG part execution",
					"proc_id",
					task.ProcId,
					"error",
					err,
				)
				return err
			}

			// recieve streamed execution results
			for {
				result, err := stream.Recv()
				// error handling
				if err != nil {
					if errors.Is(err, context.Canceled) ||
						errors.Is(err, context.DeadlineExceeded) {
						slog.Warn(
							"context done while receiving execution result",
							"proc_id",
							task.ProcId,
						)
						break
					}
					if err == io.EOF {
						slog.Info("finished receiving execution results", "proc_id", task.ProcId)
						break
					}
					slog.Error(
						"error receiving execution result",
						"proc_id",
						task.ProcId,
						"error",
						err,
					)
					return err
				}

				slog.Info("received execution result",
					"exec_id", result.GetExecId(),
				)

				var algoResultId int
				for _, algo := range algorithms {
					if (algo.Name == result.AlgorithmResult.GetAlgorithm().Name) &&
						(algo.Version == result.AlgorithmResult.GetAlgorithm().Version) {
						algoResultId = int(algo.ID)
						break
					}
				}

				// add the result in to the result map
				resultMap[int64(algoResultId)] = result

				structResult, err := convertStructToJsonBytes(
					result.AlgorithmResult.Result.GetStructValue(),
				)
				if err != nil {
					slog.Error(
						"Issue converted algorithm struct result to bytes",
						"error",
						err,
						"struct",
						result.AlgorithmResult.Result.GetStructValue(),
					)
					return err
				}

				resultId, err := d.queries.CreateResult(ctx, CreateResultParams{
					WindowsID:    pgtype.Int8{Valid: true, Int64: insertedWindow.ID},
					WindowTypeID: pgtype.Int8{Valid: true, Int64: insertedWindow.WindowTypeID},
					AlgorithmID:  pgtype.Int8{Valid: true, Int64: int64(algoResultId)},
					ResultValue: pgtype.Float8{
						Valid:   true,
						Float64: float64(result.AlgorithmResult.Result.GetSingleValue()),
					},
					ResultArray: convertFloat32ToFloat64(
						result.AlgorithmResult.Result.GetFloatValues().GetValues(),
					),
					ResultJson: structResult,
				})
				if err != nil {
					slog.Error("Error inserting result", "error", err)
					return err
				}
				slog.Info("Inserted result", "resultId", resultId)
			}
		}
	}
	return nil
}

func convertFloat32ToFloat64(float32Slice []float32) []float64 {
	float64Slice := make([]float64, len(float32Slice))
	for i, value := range float32Slice {
		float64Slice[i] = float64(value)
	}
	return float64Slice
}

func convertFloat64ToFloat32(float64Slice []float64) []float32 {
	float32Slice := make([]float32, len(float64Slice))
	for i, value := range float64Slice {
		float32Slice[i] = float32(value)
	}
	return float32Slice
}

func convertStructToJsonBytes(s *structpb.Struct) ([]byte, error) {
	return protojson.Marshal(s)
}
