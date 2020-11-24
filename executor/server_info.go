package executor

// ServerInfo command
func ServerInfo(client *Client, enableResolve bool) error {
	return RemoteCommand(client, "all", "", "server-info", "", enableResolve)
}
