// Storage-only Darwin interposer. No provider/profile data is read here.
// Non-F_FULLFSYNC fcntl calls tail-branch to libc preserving the variadic ABI.
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <libproc.h>
#include <mach-o/dyld.h>
#include <mach-o/loader.h>
#include <pthread.h>
#include <spawn.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#define INTERPOSE(replacement, original) \
__attribute__((used)) static struct { const void *r; const void *o; } \
interpose_##replacement __attribute__((section("__DATA,__interpose"))) = \
{ (const void *)&replacement, (const void *)&original }
extern int ahrb_durability_control(int);
extern int fdatasync(int) __attribute__((weak_import));
static int trace_fd = -1, self_test = 1, finished = 0;
static uint64_t sequence = 0, dropped = 0, start_time = 0;
static pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
static _Thread_local int depth, exec_depth, spawn_depth;
static uint64_t now(void) {
    struct timespec t;
    if (clock_gettime(CLOCK_MONOTONIC, &t)) return 0;
    return (uint64_t)t.tv_sec * 1000000000ULL + (uint64_t)t.tv_nsec;
}
static void record(const char *fields) {
    if (trace_fd < 0) return;
    char line[2048];
    pthread_mutex_lock(&lock);
    int n = snprintf(line, sizeof(line), "{\"sequence\":%llu,\"pid\":%d,\"start_time\":%llu,%s}\n",
        (unsigned long long)++sequence, getpid(), (unsigned long long)start_time, fields);
    if (n <= 0 || n >= (int)sizeof(line) || write(trace_fd, line, n) != n) dropped++;
    pthread_mutex_unlock(&lock);
}
static void call_record(const char *primitive, uint64_t enter, int result, int error) {
    char fields[512];
    snprintf(fields, sizeof(fields), "\"kind\":\"call\",\"primitive\":\"%s\",\"enter_ns\":%llu,\"exit_ns\":%llu,\"return_code\":%d,\"errno\":%d,\"self_test\":%s",
        primitive, (unsigned long long)enter, (unsigned long long)now(), result,
        result == -1 ? error : 0, self_test ? "true" : "false");
    record(fields);
}
static int wrap_fsync(int fd) {
    uint64_t enter = now();
    int outer = depth++ == 0;
    int result = fsync(fd), error = errno;
    depth--;
    if (outer) call_record("fsync", enter, result, error);
    errno = error;
    return result;
}
static int wrap_fdatasync(int fd) {
    uint64_t enter = now(); int outer = depth++ == 0;
    int result = fdatasync(fd), error = errno; depth--;
    if (outer) call_record("fdatasync", enter, result, error);
    errno = error; return result;
}
extern int fsync_nocancel(int) __asm("_fsync$NOCANCEL");
static int wrap_fsync_nocancel(int fd) {
    uint64_t enter = now(); int outer = depth++ == 0;
    int result = fsync_nocancel(fd), error = errno; depth--;
    if (outer) call_record("fsync", enter, result, error);
    errno = error; return result;
}
int ahrb_fullfsync(int fd) {
    uint64_t enter = now(); int outer = depth++ == 0;
    int result = fcntl(fd, F_FULLFSYNC), error = errno; depth--;
    if (outer) call_record("fullfsync", enter, result, error);
    errno = error; return result;
}
extern int fcntl_nocancel(int, int, ...) __asm("_fcntl$NOCANCEL");
int ahrb_fullfsync_nocancel(int fd) {
    uint64_t enter = now(); int outer = depth++ == 0;
    int result = fcntl_nocancel(fd, F_FULLFSYNC), error = errno; depth--;
    if (outer) call_record("fullfsync", enter, result, error);
    errno = error; return result;
}
extern int wrap_fcntl(int,int,...);
extern int wrap_fcntl_nocancel(int,int,...);
__asm__(".text\n.p2align 2\n_wrap_fcntl:\n cmp w1, #51\n b.ne 1f\n b _ahrb_fullfsync\n1: b _fcntl\n"
        ".p2align 2\n_wrap_fcntl_nocancel:\n cmp w1, #51\n b.ne 2f\n b _ahrb_fullfsync_nocancel\n2: b _fcntl$NOCANCEL\n");
extern long raw_syscall(int, ...) __asm("_syscall");
extern long wrap_syscall(int, ...);
void ahrb_syscall_gap(void) {
    record("\"kind\":\"coverage-gap\",\"reason\":\"generic syscall path outside primitive interposition\"");
}
// Darwin arm64 variadic arguments stay on the caller stack; preserve the fixed
// argument and return address while reporting the gap, then forward unchanged.
__asm__(".text\n.p2align 2\n_wrap_syscall:\n stp x29, x30, [sp, #-16]!\n"
        " stp x0, x1, [sp, #-16]!\n bl _ahrb_syscall_gap\n ldp x0, x1, [sp], #16\n"
        " ldp x29, x30, [sp], #16\n b _syscall\n");
extern long raw_syscall64(long, ...) __asm("___syscall");
extern long wrap_syscall64(long, ...);
__asm__(".text\n.p2align 2\n_wrap_syscall64:\n stp x29, x30, [sp, #-16]!\n"
        " stp x0, x1, [sp, #-16]!\n bl _ahrb_syscall_gap\n ldp x0, x1, [sp], #16\n"
        " ldp x29, x30, [sp], #16\n b ___syscall\n");
static void end(void) {
    if (finished || trace_fd < 0) return;
    finished = 1;
    char fields[160];
    snprintf(fields, sizeof(fields), "\"kind\":\"end\",\"drop_count\":%llu", (unsigned long long)dropped);
    record(fields);
}
static void wrap_exit(int code) { end(); _exit(code); }
// A fork without an exec cannot safely run a loader/control in a multithreaded
// child. Explicit coverage loss is preferable to an invented image receipt.
static pid_t wrap_fork(void) {
    record("\"kind\":\"coverage-gap\",\"reason\":\"fork requires unavailable child-image coverage\"");
    return fork();
}
static int spawn_record(int result, pid_t *pid, uint64_t enter) {
    if (!result && pid) {
        char fields[256]; snprintf(fields,sizeof(fields),"\"kind\":\"spawn\",\"child_pid\":%d,\"enter_ns\":%llu,\"exit_ns\":%llu",*pid, (unsigned long long)enter, (unsigned long long)now()); record(fields);
    }
    return result;
}
static int wrap_spawn(pid_t *pid, const char *path, const posix_spawn_file_actions_t *actions,
 const posix_spawnattr_t *attr, char *const argv[], char *const envp[]) {
    // POSIX permits callers to omit the PID output. Coverage still needs it.
    pid_t local_pid;
    if (!pid) pid=&local_pid;
    uint64_t enter=now(); int outer=spawn_depth++ == 0;
    int result=posix_spawn(pid,path,actions,attr,argv,envp);
    spawn_depth--;
    return outer ? spawn_record(result,pid,enter) : result;
}
static int wrap_spawnp(pid_t *pid, const char *path, const posix_spawn_file_actions_t *actions,
 const posix_spawnattr_t *attr, char *const argv[], char *const envp[]) {
    // POSIX permits callers to omit the PID output. Coverage still needs it.
    pid_t local_pid;
    if (!pid) pid=&local_pid;
    uint64_t enter=now(); int outer=spawn_depth++ == 0;
    int result=posix_spawnp(pid,path,actions,attr,argv,envp);
    spawn_depth--;
    return outer ? spawn_record(result,pid,enter) : result;
}
static int wrap_execve(const char *path, char *const argv[], char *const envp[]) {
    // A successful exec loses this image's destructor. Require the new load
    // receipt as the end-of-image witness, and preserve failed exec explicitly.
    int outer=exec_depth++ == 0;
    if (outer) record("\"kind\":\"exec\"");
    int result=execve(path,argv,envp), error=errno;
    exec_depth--;
    if (outer) record("\"kind\":\"exec-failed\"");
    errno=error; return result;
}
static int wrap_execv(const char *path, char *const argv[]) {
    int outer=exec_depth++ == 0;
    if (outer) record("\"kind\":\"exec\"");
    int result=execv(path,argv), error=errno;
    exec_depth--;
    if (outer) record("\"kind\":\"exec-failed\"");
    errno=error; return result;
}
static int wrap_execvp(const char *path, char *const argv[]) {
    int outer=exec_depth++ == 0;
    if (outer) record("\"kind\":\"exec\"");
    int result=execvp(path,argv), error=errno;
    exec_depth--;
    if (outer) record("\"kind\":\"exec-failed\"");
    errno=error; return result;
}
// Check application code, including statically linked dependencies, for direct
// supervisor calls. System syscall stubs are covered by the libc ABI hooks above.
static void image_added(const struct mach_header *header, intptr_t slide) {
    Dl_info info;
    if (!dladdr(header,&info) || !info.dli_fname) {
        record("\"kind\":\"coverage-gap\",\"reason\":\"unresolved loaded image\""); return;
    }
    const char *path=info.dli_fname;
    if (!strncmp(path,"/usr/lib/",9) || !strncmp(path,"/System/Library/",16)) return;
    if (header->magic != MH_MAGIC_64 || header->cputype != CPU_TYPE_ARM64) {
        record("\"kind\":\"coverage-gap\",\"reason\":\"unsupported image architecture\""); return;
    }
    const struct mach_header_64 *h=(const void *)header;
    const struct load_command *lc=(const void *)(h+1);
    for (uint32_t i=0;i<h->ncmds;i++,lc=(const void *)((const char *)lc+lc->cmdsize)) {
        if (lc->cmd != LC_SEGMENT_64) continue;
        const struct segment_command_64 *seg=(const void *)lc;
        const struct section_64 *sec=(const void *)(seg+1);
        for(uint32_t j=0;j<seg->nsects;j++) {
            if (!(sec[j].flags & (S_ATTR_PURE_INSTRUCTIONS|S_ATTR_SOME_INSTRUCTIONS))) continue;
            const uint32_t *code=(const void *)(sec[j].addr+slide);
            for(uint64_t k=0;k<sec[j].size/4;k++) {
                if ((code[k] & 0xffe0001fU)==0xd4000001U) {
                    record("\"kind\":\"coverage-gap\",\"reason\":\"direct supervisor instruction outside interpose coverage\""); return;
                }
            }
        }
    }
}
__attribute__((constructor)) static void begin(void) {
    const char *root=getenv("AHRB_DURABILITY_TRACE");
    if (!root) return;
    struct proc_bsdinfo b;
    if (proc_pidinfo(getpid(),PROC_PIDTBSDINFO,0,&b,sizeof(b)) != sizeof(b)) return;
    start_time=(uint64_t)b.pbi_start_tvsec*1000000ULL+b.pbi_start_tvusec;
    char path[4096], exe[PROC_PIDPATHINFO_MAXSIZE], fields[512];
    uint64_t image=now();
    snprintf(path,sizeof(path),"%s/%d-%llu-%llu.jsonl",root,getpid(),(unsigned long long)start_time,(unsigned long long)image);
    trace_fd=open(path,O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);
    if (trace_fd<0) return;
    // Keep the executable path as a separate byte receipt, avoiding JSON escapes.
    if (proc_pidpath(getpid(),exe,sizeof(exe)) <= 0) {
        record("\"kind\":\"coverage-gap\",\"reason\":\"missing executable path\"");
    } else {
        strlcat(path,".image",sizeof(path));
        int fd=open(path,O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);
        if(fd<0 || write(fd,exe,strlen(exe))!=(ssize_t)strlen(exe)) dropped++;
        if(fd>=0)close(fd);
    }
    record("\"kind\":\"load\",\"version\":\"darwin-arm64-interpose-v1\"");
    _dyld_register_func_for_add_image(image_added);
    snprintf(path,sizeof(path),"%s/control-%d-%llu",root,getpid(),(unsigned long long)image);
    int fd=open(path,O_RDWR|O_CREAT|O_EXCL,0600);
    if (fd>=0) write(fd,"control",7);
    int failures=fd<0 ? 1 : ahrb_durability_control(fd);
    if(fd>=0)close(fd);
    unlink(path);
    snprintf(fields,sizeof(fields),"\"kind\":\"control\",\"failures\":%d,\"fdatasync_available\":%s",failures,fdatasync ? "true":"false");record(fields);
    self_test=0;
    if(getenv("AHRB_DURABILITY_CONTROL_ONLY")) {end(); _exit(failures ? 78 : 0);}
}
__attribute__((destructor)) static void finish(void) {end();}
INTERPOSE(wrap_fsync,fsync);
INTERPOSE(wrap_fdatasync,fdatasync);
INTERPOSE(wrap_fsync_nocancel,fsync_nocancel);
INTERPOSE(wrap_fcntl,fcntl);
INTERPOSE(wrap_fcntl_nocancel,fcntl_nocancel);
INTERPOSE(wrap_exit,_exit);
INTERPOSE(wrap_fork,fork);
INTERPOSE(wrap_spawn,posix_spawn);
INTERPOSE(wrap_spawnp,posix_spawnp);
INTERPOSE(wrap_execve,execve);

INTERPOSE(wrap_execv,execv);
INTERPOSE(wrap_execvp,execvp);
INTERPOSE(wrap_syscall,raw_syscall);

INTERPOSE(wrap_syscall64,raw_syscall64);
