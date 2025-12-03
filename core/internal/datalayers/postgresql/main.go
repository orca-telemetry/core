package postgresql

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5/pgtype"

	"github.com/orc-analytics/orca/core/internal/dag"
	pb "github.com/orc-analytics/orca/core/protobufs/go"
)

// RegisterProcessor with Orca Core
func (d *Datalayer) RegisterProcessor(
	ctx context.Context,
	proc *pb.ProcessorRegistration,
) error {
	slog.Debug("registering processor", "processor", proc)

	tx, err := d.WithTx(ctx)

	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	if err != nil {
		slog.Error("could not start a transaction", "error", err)
		return err
	}

	// register the processor
	err = d.createProcessor(ctx, tx, proc)

	if err != nil {
		slog.Error("could not create processor", "error", err)
		return err
	}

	// add all algorithms first
	for _, algo := range proc.GetSupportedAlgorithms() {
		// add window types
		windowType := algo.GetWindowType()

		// create / update the window type
		windowTypeId, err := d.createWindowType(ctx, tx, windowType)
		if err != nil {
			return err
		}

		// read any existing metadata fields for the window
		metadataFieldsAsStored, err := d.readMetadataFieldsByWindowType(ctx, tx, windowType)
		if err != nil {
			return err
		}

		// if there are existing fields, check they are the same as the provided window
		// just check on metadatafield name
		if len(metadataFieldsAsStored) > 0 {
			if len(windowType.MetadataFields) != len(metadataFieldsAsStored) {
				return fmt.Errorf(
					`Metadata fields of incoming window type %v, do not match the
					number of fields stored in the database for this window.
					Expected: %v, got %v. Considering bumping the version of the
					window type.`, windowType, metadataFieldsAsStored, windowType.MetadataFields,
				)
			}
			metadataFieldNamesAsStored := make([]string, len(metadataFieldsAsStored))
			for ii, field := range metadataFieldsAsStored {
				metadataFieldNamesAsStored[ii] = field.GetName()
			}
			for _, metadataField := range windowType.MetadataFields {
				if !slices.Contains(metadataFieldNamesAsStored, metadataField.GetName()) {
					return fmt.Errorf(
						`Recieved a metadata field %v of window type %v that is not registered
						in the database. If you want to keep this field, bump the version
						of the window type.`, metadataField.GetName(), windowType,
					)
				}
			}
		} else {
			var metadataFieldIds []int64
			for _, metadataField := range windowType.MetadataFields {
				metadataFieldId, err := d.createMetadataField(ctx, tx, metadataField)
				if err != nil {
					return fmt.Errorf("sql issue creating the metadata field: %v", err)
				}

				err = d.createMetadataFieldBridge(ctx, tx, windowTypeId, metadataFieldId)
				if err != nil {
					return fmt.Errorf("sql issue in creating the metadata field bridge: %v", err)
				}
				metadataFieldIds = append(metadataFieldIds, metadataFieldId)
			}
		}

		// create algos
		err = d.addAlgorithm(ctx, tx, algo, proc)
		if err != nil {
			slog.Error("error creating algorithm", "error", err)
			return err
		}
	}

	// then add the dependencies and associate the processor with all the algos
	for _, algo := range proc.GetSupportedAlgorithms() {
		err := d.addOverwriteAlgorithmDependency(
			ctx,
			tx,
			algo,
			proc,
		)
		if err != nil {
			// error wrapping is important here because we return some custom errors
			return fmt.Errorf("issue adding algorithm dependency: %w", err)
		}
	}

	return tx.Commit(ctx)
}

// EmitWindow with Orca core
func (d *Datalayer) EmitWindow(
	ctx context.Context,
	window *pb.Window,
) (pb.WindowEmitStatus, error) {
	slog.Debug("recieved emitted window", "window", window)

	tx, err := d.WithTx(ctx)

	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	if err != nil {
		slog.Error("could not start a transaction", "error", err)
		return pb.WindowEmitStatus{}, err
	}

	pgTx := tx.(*PgTx)
	qtx := d.queries.WithTx(pgTx.tx)

	// marshal metadata
	metadata := window.GetMetadata()
	metadataBytes, err := metadata.MarshalJSON()
	if err != nil {
		return pb.WindowEmitStatus{}, fmt.Errorf("could not marshal metadata: %v", err)
	}

	// check whether metadata is needed
	metadataFields, err := qtx.ReadMetadataFieldsByWindowType(ctx, ReadMetadataFieldsByWindowTypeParams{
		WindowTypeName:    window.GetWindowTypeName(),
		WindowTypeVersion: window.GetWindowTypeVersion(),
	})
	if err != nil {
		return pb.WindowEmitStatus{}, fmt.Errorf("could not read metadata for window: %v", err)
	}

	// confident that any required metadata is being supplied to the processor
	if len(metadataFields) > 0 {
		var metadataMap map[string]any
		if err := json.Unmarshal(metadataBytes, &metadataMap); err != nil {
			return pb.WindowEmitStatus{}, fmt.Errorf("could not unmarshal metadata for validation: %v", err)
		}

		for _, mDataField := range metadataFields {
			fieldName := mDataField.MetadataFieldName
			if _, exists := metadataMap[fieldName]; !exists {
				return pb.WindowEmitStatus{}, fmt.Errorf("required metadata field '%s' is missing", fieldName)
			}
		}
	}

	insertedWindow, err := qtx.RegisterWindow(ctx, RegisterWindowParams{
		WindowTypeName:    window.GetWindowTypeName(),
		WindowTypeVersion: window.GetWindowTypeVersion(),
		TimeFrom: pgtype.Timestamp{
			Time:  window.GetTimeFrom().AsTime().UTC(),
			Valid: true,
		},
		TimeTo: pgtype.Timestamp{
			Time:  window.GetTimeTo().AsTime().UTC(),
			Valid: true,
		},
		Origin:   window.GetOrigin(),
		Metadata: metadataBytes,
	})
	if err != nil {
		slog.Error("could not insert window", "error", err)
		if strings.Contains(err.Error(), "(SQLSTATE 23503)") {
			return pb.WindowEmitStatus{}, fmt.Errorf(
				"window type does not exist - insert via window type registration: %v",
				err.Error(),
			)
		}
	}
	slog.Debug("window record inserted into the datalayer", "window", insertedWindow)
	execPaths, err := qtx.ReadAlgorithmExecutionPaths(
		ctx,
		strconv.Itoa(int(insertedWindow.WindowTypeID)),
	)
	if err != nil {
		slog.Error(
			"could not read execution paths for window id",
			"window_id",
			insertedWindow,
			"error",
			err,
		)
		return pb.WindowEmitStatus{Status: pb.WindowEmitStatus_TRIGGERING_FAILED}, err
	}

	// create the algo path args
	var algoIDPaths []string
	var windowTypeIDPaths []string
	var procIDPaths []string
	for _, path := range execPaths {
		algoIDPaths = append(algoIDPaths, path.AlgoIDPath)
		windowTypeIDPaths = append(windowTypeIDPaths, path.WindowTypeIDPath)
		procIDPaths = append(procIDPaths, path.ProcIDPath)
	}

	// fire off processings
	executionPlan, err := dag.BuildPlan(
		algoIDPaths,
		windowTypeIDPaths,
		procIDPaths,
		int64(insertedWindow.WindowTypeID),
	)
	if err != nil {
		slog.Error(
			"failed to construct execution paths for window",
			"window",
			insertedWindow,
			"error",
			err,
		)
		return pb.WindowEmitStatus{Status: pb.WindowEmitStatus_TRIGGERING_FAILED}, err
	}

	if len(executionPlan.Stages) > 0 {
		go processTasks(d, executionPlan, window, insertedWindow)

		return pb.WindowEmitStatus{
			Status: pb.WindowEmitStatus_PROCESSING_TRIGGERED,
		}, tx.Commit(ctx)
	}
	return pb.WindowEmitStatus{
		Status: pb.WindowEmitStatus_NO_TRIGGERED_ALGORITHMS,
	}, nil
}

func (d *Datalayer) Expose(
	ctx context.Context,
	_ *pb.ExposeSettings,
) (*pb.InternalState, error) {
	// settings not handled for now

	tx, err := d.WithTx(ctx)

	defer func() {
		if tx != nil {
			tx.Rollback(ctx)
		}
	}()

	if err != nil {
		slog.Error("could not start a transaction", "error", err)
		return nil, err
	}

	pgTx := tx.(*PgTx)

	qtx := d.queries.WithTx(pgTx.tx)

	// read all the algorithms
	algorithms, err := qtx.ReadAlgorithms(ctx)
	if err != nil {
		slog.Error("could not read algorithms", "error", err)
		return nil, fmt.Errorf("could not read algorithms: %w", err)
	}
	algosMap := make(map[int]Algorithm, len(algorithms))
	for ii, algo := range algorithms {
		// pack 'em into the map
		algosMap[ii] = algo
	}

	// get read all the window types
	wts, err := qtx.ReadWindowTypes(ctx)
	if err != nil {
		slog.Error("could not read window types", "error", err)
		return nil, fmt.Errorf("could not read window types: %w", err)
	}
	wtsMap := make(map[int64]WindowType, len(wts))
	for _, wt := range wts {
		wtsMap[wt.ID] = wt
	}

	algorithmsPb := make([]*pb.Algorithm, len(algorithms))
	for jj, algo := range algorithms {
		// get the window type for this algorithm
		wt, ok := wtsMap[algo.WindowTypeID]
		if !ok {
			slog.Error("could not find the window type id, which algorithm depends on", "window_type_id", algo.WindowTypeID, "algorithm_id", algo.ID)
			return nil, fmt.Errorf("could not find the window type that algorithm %v, depends on", algo.Name)
		}
		// parse out the result type
		var resultType pb.ResultType
		switch algo.ResultType {
		case ResultTypeStruct:
			resultType = pb.ResultType_STRUCT
		case ResultTypeValue:
			resultType = pb.ResultType_VALUE
		case ResultTypeNone:
			resultType = pb.ResultType_NONE
		case ResultTypeArray:
			resultType = pb.ResultType_ARRAY
		default:
			resultType = pb.ResultType_NOT_SPECIFIED
		}

		algorithmsPb[jj] = &pb.Algorithm{
			Name:    algo.Name,
			Version: algo.Version,
			WindowType: &pb.WindowType{
				Name:        wt.Name,
				Version:     wt.Version,
				Description: wt.Description,
			},
			ResultType:  resultType,
			Description: algo.Description,
		}
	}

	return &pb.InternalState{
		Algorithms: algorithmsPb,
	}, nil

}
