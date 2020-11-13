package commands

import (
	"fmt"
	"strings"

	"github.com/gobwas/glob/syntax/ast"
)

// adminCommand is the interface for all the supported commands in admin-cli.
type pegicCommand interface {

	// Execute the command
	execute(parsedCmd *ast.ParsedCommand) error

	// Create a AST node for this command.
	astNode() *ast.CommandASTNode
}

var commandsTable = map[string]pegicCommand{
	"USE": &useCommand{},
}

// Executor is the admin-cli command executor in interactive mode.
func Executor(s string) {
	s = strings.TrimSpace(s)
	if len(s) == 0 {
		return
	}

	args := strings.Split(s, " ")
	cmdStr := strings.ToUpper(args[0])
	cmd, found := commandsTable[cmdStr]
	if !found {
		fmt.Printf("ERROR: unsupported command: \"%s\"\n", cmdStr)
		return
	}
	parsedCmd, err := ast.Parse(args)
	if err != nil {
		fmt.Printf("ERROR: unable to parse command: %s\n", err)
		return
	}
	if err := cmd.execute(parsedCmd); err != nil {
		fmt.Printf("ERROR: execution failed: %s\n", err)
		return
	}
}
