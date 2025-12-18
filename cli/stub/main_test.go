package stub

import (
	"bytes"
	"strings"
	"testing"
)

func TestPythonTemplateGeneration(t *testing.T) {
	testData := ProcessorData{
		Name: "ml-test",
		Metadata: []Metadata{
			{VarName: "bus_id", KeyName: "bus_id", Description: "Unique bus ID"},
		},
		Windows: []Window{
			{
				VarName:          "FastWindow",
				Name:             "FastWindow",
				Version:          "1.0.0",
				MetadataVarNames: []string{"bus_id"},
			},
		},
		Algorithms: []Algorithm{
			{
				Name:          "SpeedCheck",
				Version:       "1.1.0",
				WindowVarName: "FastWindow",
				ReturnType:    "StructResult",
				MetadataKeys:  []AlgoMetadata{{VarName: "bus_id", KeyName: "bus_id"}},
			},
		},
	}

	var buf bytes.Buffer
	err := pythonTemplate.Execute(&buf, testData)
	if err != nil {
		t.Fatalf("Template execution failed: %v", err)
	}

	output := buf.String()

	assertions := []struct {
		name     string
		contains string
	}{
		{"Processor Init", `proc = Processor(`},
		{"Metadata Field", `bus_id = MetadataField(`},
		{"Window Type", `FastWindow = WindowType(`},
		{"Window Metadata Link", `metadataFields=[bus_id]`},
		{"Algorithm Decorator", `@proc.algorithm("SpeedCheck", "1.1.0", FastWindow, _stub=True)`},
		{"Function Definition", `def speed_check(params: ExecutionParams) -> StructResult:`},
		{"Metadata Retrieval", `bus_id = params.window.metadata.get("bus_id", None)`},
	}

	for _, a := range assertions {
		if !strings.Contains(output, a.contains) {
			t.Errorf("Assertion Failed [%s]: Output did not contain expected string: %s", a.name, a.contains)
		}
	}

	t.Logf("Generated Python:\n%s", output)
}

func TestPythonTemplateGeneration_WithReturnTypes(t *testing.T) {
	testData := ProcessorData{
		Name: "ml-test",
		Algorithms: []Algorithm{
			{
				Name:          "CalcAverage",
				ReturnType:    pythonValueReturnType,
				WindowVarName: "Every30Second",
			},
			{
				Name:          "GetBatch",
				ReturnType:    pythonStructReturnType,
				WindowVarName: "Every30Second",
			},
			{
				Name:          "SendResult",
				ReturnType:    pythonNoneReturnType,
				WindowVarName: "Every30Second",
			},
			{
				Name:          "CalcDist",
				ReturnType:    pythonArrayReturnType,
				WindowVarName: "Every30Second",
			},
		},
	}

	var buf bytes.Buffer
	pythonTemplate.Execute(&buf, testData)
	output := buf.String()

	assertions := []struct {
		name     string
		contains string
	}{
		{"ValueResult Signature", "def calc_average(params: ExecutionParams) -> ValueResult:"},
		{"ValueResult Return", "return ValueResult(0)"},
		{"StructResult Signature", "def get_batch(params: ExecutionParams) -> StructResult:"},
		{"StructResult Return", `return StructResult({"result": 0})`},
		{"ArrayResult Signature", "def calc_dist(params: ExecutionParams) -> ArrayResult:"},
		{"ArrayResult Return", "return ArrayResult([])"},
		{"Noneresult Signature", "def send_result(params: ExecutionParams) -> NoneResult:"},
		{"NoneResult Return", "return NoneResult()"},
	}

	for _, a := range assertions {
		if !strings.Contains(output, a.contains) {
			t.Errorf("Failed [%s]: Expected to find %s", a.name, a.contains)
		}
	}

	t.Logf("Generated Python:\n%s", output)
}
