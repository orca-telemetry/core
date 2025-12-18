package stub

import (
	"embed"
	"os"
	"path/filepath"
	"text/template"

	pb "github.com/orc-analytics/orca/core/protobufs/go"
)

const PYTHON_STUB_FILE = "stub_templates/processor.py.tmpl"

//go:embed stub_templates/*.tmpl
var templateFS embed.FS

var pythonTemplate *template.Template

type pythonReturnType string

const (
	pythonStructReturnType pythonReturnType = "StructResult"
	pythonValueReturnType  pythonReturnType = "ValueResult"
	pythonNoneReturnType   pythonReturnType = "NoneResult"
	pythonArrayReturnType  pythonReturnType = "ArrayResult"
)

func init() {
	baseName := filepath.Base(PYTHON_STUB_FILE)
	pythonTemplate = template.Must(template.New(baseName).Funcs(
		template.FuncMap{
			"ToSnakeCase": toSnakeCase,
		}).ParseFS(templateFS, PYTHON_STUB_FILE))
}

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
	ReturnType    pythonReturnType
	MetadataKeys  []AlgoMetadata
}

type ProcessorData struct {
	Name       string
	Metadata   []Metadata
	Windows    []Window
	Algorithms []Algorithm
}

func GeneratePythonStub(internalState *pb.InternalState, outDir string) error {

	outFile, err := os.Create("orca_stub.py")

	if err != nil && !os.IsExist(err) {
		return err
	}

	defer outFile.Close()

	err = os.Mkdir(outDir, 0750)

	if err != nil && !os.IsExist(err) {
		return (err)
	}
	if err := pythonTemplate.Execute(outFile, internalState); err != nil {
		panic(err)
	}
	println("Generated system_state.py successfully!")
	return nil
}
