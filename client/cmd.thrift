namespace go client

struct command
{
    1:string cmd;
    2:list<string> arguments;
}

service remoteCmdService {
    string callCommand(1:command cmd);
}
