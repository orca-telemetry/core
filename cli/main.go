package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	pb "github.com/orc-analytics/orca/core/protobufs/go"

	"github.com/orc-analytics/orca/cli/stub"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	// Define subcommands
	startCmd := flag.NewFlagSet("start", flag.ExitOnError)
	stopCmd := flag.NewFlagSet("stop", flag.ExitOnError)
	statusCmd := flag.NewFlagSet("status", flag.ExitOnError)
	destroyCmd := flag.NewFlagSet("destroy", flag.ExitOnError)
	helpCmd := flag.NewFlagSet("help", flag.ExitOnError)

	// Check if a subcommand is provided
	if len(os.Args) < 2 {
		fmt.Println()
		showHelp()
		fmt.Println()
		os.Exit(1)
	}

	// Parse the appropriate subcommand
	switch os.Args[1] {

	case "start":
		checkDockerInstalled()

		startCmd.Parse(os.Args[2:])

		fmt.Println()
		networkName := createNetworkIfNotExists()
		fmt.Println()

		startPostgres(networkName)
		fmt.Println()

		startRedis(networkName)
		fmt.Println()

		// check for postgres instance running first
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*15)
		defer cancel()
		err := waitForPgReady(ctx, pgContainerName, time.Millisecond*500)
		if err != nil {
			fmt.Println(
				renderError(
					fmt.Sprintf("Issue waiting for Postgres store to start: %v", err.Error()),
				),
			)
			os.Exit(1)
		}
		startOrca(networkName)
		fmt.Println()

		fmt.Println(renderSuccess("✅ Orca stack started successfully."))
		fmt.Println()

	case "stop":
		checkDockerInstalled()

		stopCmd.Parse(os.Args[2:])

		fmt.Println()
		stopContainers()

		fmt.Println()
		fmt.Println(renderSuccess("✅ All containers stopped."))
		fmt.Println()

	case "status":
		checkDockerInstalled()
		statusCmd.Parse(os.Args[2:])

		fmt.Println()
		showStatus()
		fmt.Println()

	case "destroy":
		checkDockerInstalled()
		destroyCmd.Parse(os.Args[2:])
		fmt.Println()
		destroy()
		fmt.Println()

	case "stub":
		stubCmd := flag.NewFlagSet("stub", flag.ExitOnError)
		outDir := stubCmd.String("out", "./orca-stubs", "Output directory for generated stubs")
		orcaConnStr := stubCmd.String("connStr", "", "Orca connection string")

		args := os.Args[2:]
		sdkIndex := -1
		for i, arg := range args {
			if !strings.HasPrefix(arg, "-") {
				sdkIndex = i
				break
			}
		}

		if sdkIndex == -1 {
			fmt.Println(renderError("The SDK target needs to be provided (e.g. python, go, js)"))
			os.Exit(1)
		}

		sdkLanguage := args[sdkIndex]

		// remove the SDK language from args and parse the rest as flags
		flagArgs := append(args[:sdkIndex], args[sdkIndex+1:]...)
		stubCmd.Parse(flagArgs)

		validSDKs := map[string]bool{
			"python": true,
		}

		if !validSDKs[sdkLanguage] {
			fmt.Println(renderError(fmt.Sprintf("Unsupported SDK language: %s", sdkLanguage)))
			fmt.Println(renderInfo("Supported languages: python"))
			os.Exit(1)
		}

		var connStr string
		if *orcaConnStr == "" {
			orcaStatus := getContainerStatus(orcaContainerName)

			if orcaStatus == "running" {
				orcaPort := getContainerPort(orcaContainerName, 3335)
				connStr = fmt.Sprintf("localhost:%s", orcaPort)
			}
		} else {
			connStr = *orcaConnStr
		}

		fmt.Println()
		fmt.Printf("Generating %s stubs to %s...\n", sdkLanguage, *outDir)

		if err := os.MkdirAll(*outDir, 0755); err != nil {
			fmt.Println(renderError(fmt.Sprintf("Failed to create output directory: %v", err)))
			os.Exit(1)
		}
		conn, err := grpc.NewClient(connStr, grpc.WithTransportCredentials(insecure.NewCredentials()))
		defer conn.Close()
		if err != nil {
			fmt.Println(renderError(fmt.Sprintf("Issue preparping to contact Orca: %v", err)))
			os.Exit(1)
		}

		orcaCoreClient := pb.NewOrcaCoreClient(conn)
		internalState, err := orcaCoreClient.Expose(context.Background(), &pb.ExposeSettings{})

		if err != nil {
			fmt.Println(renderError(fmt.Sprintf("Issue contacting Orca: %v", err)))
			os.Exit(1)
		}

		if sdkLanguage == "python" {
			err := stub.GeneratePythonStub(internalState, *outDir)
			if err != nil {
				fmt.Println(renderError(fmt.Sprintf("Failed to generate python stubs: %v", err)))
				os.Exit(1)
			}
		}
		fmt.Println(renderSuccess(fmt.Sprintf("✅ %s stubs generated successfully in %s", sdkLanguage, *outDir)))
		fmt.Println()

	case "help":
		helpCmd.Parse(os.Args[2:])
		fmt.Println()
		if helpCmd.NArg() > 0 {
			showCommandHelp(os.Args[2])
		} else {
			showHelp()
		}
		fmt.Println()

	default:
		fmt.Println()
		fmt.Println(renderError(fmt.Sprintf("Unknown subcommand: %s", os.Args[1])))
		fmt.Println(renderInfo("Run 'help' for usage information."))
		fmt.Println()
		os.Exit(1)
	}
}
