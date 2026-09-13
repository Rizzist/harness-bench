#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
extern char **environ;
int main(int argc, char **argv) {
    if (argc > 1 && !strcmp(argv[1], "child")) {
        char path[] = "spawn-control-XXXXXX";
        int fd = mkstemp(path);
        if (fd < 0) return 10;
        int result = fsync(fd);
        close(fd); unlink(path);
        return result ? 11 : 0;
    }
    if (argc > 1 && !strcmp(argv[1], "vfork-strip")) {
        char *empty_environment[] = {NULL};
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wdeprecated-declarations"
        pid_t child = vfork();
#pragma clang diagnostic pop
        if (child < 0) return 30;
        if (!child) {
            execle(argv[0], argv[0], "child", (char *)NULL, empty_environment);
            _exit(31);
        }
        int status = 0;
        if (waitpid(child,&status,0) != child || !WIFEXITED(status) || WEXITSTATUS(status)) return 32;
        return 0;
    }
    for (int i=0; i<4; i++) {
        pid_t child = 0;
        char *args[] = {argv[0], "child", NULL};
        // Both entry points permit a NULL PID output. The parent still waits
        // for each short-lived child so the fixture has no detached workload.
        pid_t *output = i < 2 ? &child : NULL;
        char *empty_environment[] = {NULL};
        char **environment = argc > 1 && i >= 2 ? empty_environment : environ;
        int result = i % 2 ? posix_spawnp(output,argv[0],NULL,NULL,args,environment)
                           : posix_spawn(output,argv[0],NULL,NULL,args,environment);
        if (result) return 20;
        int status = 0;
        pid_t waited = waitpid(output ? child : -1,&status,0);
        if (waited <= 0 || (output && waited != child) || !WIFEXITED(status) || WEXITSTATUS(status)) return 21;
    }
    return 0;
}
