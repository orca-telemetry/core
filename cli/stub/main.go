package stub

import (
	"html/template"
	"os"

	pb "github.com/orc-analytics/orca/core/protobufs/go"
)

func toSnakeCase(s string) string {
	var result []rune
	for i, r := range s {
		if i > 0 && r >= 'A' && r <= 'Z' {
			result = append(result, '_')
		}
		if r >= 'A' && r <= 'Z' {
			result = append(result, r+32)
		} else {
			result = append(result, r)
		}
	}
	return string(result)
}

// data structures matching the template expectations
type Metadata struct {
	VarName     string
	KeyName     string
	Description string
}

type Window struct {
	VarName          string
	Name             string
	Version          string
	Description      string
	MetadataVarNames []string
}

type AlgoMetadata struct {
	VarName string
	KeyName string
}

type Algorithm struct {
	Name          string
	Version       string
	WindowVarName string
	SnakeName     string
	MetadataKeys  []AlgoMetadata
}

type ProcessorData struct {
	Name       string
	Metadata   []Metadata
	Windows    []Window
	Algorithms []Algorithm
}

func GeneratePythonStub(internalState *pb.InternalState, outDir string) error {

	// load template from file with custom functions
	tmpl := template.Must(template.New("./stub_templates/python.tmpl").Funcs(template.FuncMap{
		"ToSnakeCase": toSnakeCase,
	}).ParseFiles("./stub_templates/python.tmpl"))

	outFile, err := os.Create("system_state.py")

	if err != nil && !os.IsExist(err) {
		return err
	}

	defer outFile.Close()

	err = os.Mkdir(outDir, 0750)

	if err != nil && !os.IsExist(err) {
		return (err)
	}
	if err := tmpl.Execute(outFile, internalState); err != nil {
		panic(err)
	}
	println("Generated system_state.py successfully!")
	return nil
}
