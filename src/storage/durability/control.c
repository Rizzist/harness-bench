// Independent dynamically linked known-call fixture. It never writes receipts.
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
extern int fdatasync(int) __attribute__((weak_import));
extern int fsync_nocancel(int) __asm("_fsync$NOCANCEL");
extern int fcntl_nocancel(int, int, ...) __asm("_fcntl$NOCANCEL");
int ahrb_durability_control(int fd) {
    int failures = 0;
    // Exercise no-argument, integer and pointer fcntl forwarding as well.
    failures += fcntl(fd, F_GETFD) < 0;
    failures += fcntl(fd, F_SETFD, FD_CLOEXEC) != 0;
    struct flock query = { .l_type = F_WRLCK, .l_whence = SEEK_SET };
    failures += fcntl(fd, F_GETLK, &query) != 0;
    failures += fsync(fd) != 0;
    failures += fsync(fd) != 0;
    failures += (fsync(-1) != -1 || errno != EBADF);
    failures += fsync_nocancel(fd) != 0;
    failures += (fsync_nocancel(-1) != -1 || errno != EBADF);
    if (fdatasync) {
        failures += fdatasync(fd) != 0;
        failures += (fdatasync(-1) != -1 || errno != EBADF);
    }
    failures += fcntl(fd, F_FULLFSYNC) != 0;
    failures += (fcntl(-1, F_FULLFSYNC) != -1 || errno != EBADF);
    failures += fcntl_nocancel(fd, F_FULLFSYNC) != 0;
    failures += (fcntl_nocancel(-1, F_FULLFSYNC) != -1 || errno != EBADF);
    return failures;
}
