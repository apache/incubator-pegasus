#include <cstdio>
#include <cstring>

#include "data_verifier.h"
#include "process_killer.h"

int main(int argc, const char **argv)
{
    if (argc < 3) {
        printf("invalid arguments: pegasus_kill_test configfile worker_type(verifier|killer)\n");
        return -1;
    } else if (strcmp(argv[2], "verifier") == 0) {
        verifier_initialize(argv[1]);
        verifier_start();
    } else if (strcmp(argv[2], "killer") == 0) {
        killer_initialize(argv[1]);
        killer_start();
    } else {
        printf("invalid worker_type: %s\n", argv[2]);
        return -1;
    }

    return 0;
}
