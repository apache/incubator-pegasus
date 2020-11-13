package main

import (
	"fmt"
	"net"

	"github.com/c-bata/go-prompt"
	"github.com/spf13/pflag"
)

var metaServerList *[]string

func init() {
	metaServerList = pflag.StringArray("meta", nil, "a list of ip:port of MetaServer")
}

func main() {
	pflag.Parse()

	// validate --meta
	if metaServerList == nil || len(*metaServerList) == 0 {
		pflag.Usage()
		return
	}
	for _, metaIPPort := range *metaServerList {
		_, err := net.ResolveTCPAddr("tcp4", metaIPPort)
		if err != nil {
			fmt.Printf("Invalid MetaServer TCP address [%s]\n", err)
			pflag.Usage()
			return
		}
	}

	p := prompt.New(
		pegic.Executor,
		pegic.Completer,
		prompt.OptionPrefix(">>> "),
		prompt.OptionInputTextColor(prompt.Yellow),
		prompt.OptionPreviewSuggestionTextColor(prompt.Green),
		prompt.OptionPrefixTextColor(prompt.Blue))
	p.Run()
}
