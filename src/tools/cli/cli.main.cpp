// apps
# include "cli_app.h"

int main(int argc, char** argv)
{
    // register all possible service apps
    dsn::register_app<::dsn::service::cli>("cli");

    // specify what services and tools will run in config file, then run
    dsn_run_config("config.ini", true);
    return 0;
}
