package stub

import (
	"bytes"
	"strings"
	"testing"
	"text/template"
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
				SnakeName:     "speed_check",
				MetadataKeys:  []AlgoMetadata{{VarName: "bus_id", KeyName: "bus_id"}},
			},
		},
	}

	tmpl, err := template.ParseFiles("./stub_templates/processor.py.tmpl")
	if err != nil {
		t.Fatalf("Failed to parse template file: %v", err)
	}

	var buf bytes.Buffer
	err = tmpl.Execute(&buf, testData)
	if err != nil {
		t.Fatalf("Template execution failed: %v", err)
	}

	output := buf.String()

	assertions := []struct {
		name     string
		contains string
	}{
		{"Processor Init", `proc = Processor("ml-test")`},
		{"Metadata Field", `bus_id = MetadataField(name="bus_id"`},
		{"Window Type", `FastWindow = WindowType(`},
		{"Window Metadata Link", `metadataFields=[bus_id]`},
		{"Algorithm Decorator", `@proc.algorithm("SpeedCheck", "1.1.0", FastWindow)`},
		{"Function Definition", `def speed_check(params: ExecutionParams) -> StructResult:`},
		{"Metadata Retrieval", `bus_id = params.window.metadata.get("bus_id", None)`},
		{"Main Block", `if __name__ == "__main__":`},
	}

	for _, a := range assertions {
		if !strings.Contains(output, a.contains) {
			t.Errorf("Assertion Failed [%s]: Output did not contain expected string: %s", a.name, a.contains)
		}
	}

	t.Logf("Generated Python:\n%s", output)
}
