// Linux-only deterministic I/O throttle. Delay the first journal fsync for each
// task-owned database, then call the real fsync: durability is never bypassed.
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

static pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
static char seen[1024][PATH_MAX];
static size_t count;

static void throttle(int fd) {
    const char *root = getenv("SQLPOOL1_ROOT");
    if (!root || !*root) return;
    char link[64], path[PATH_MAX];
    snprintf(link, sizeof(link), "/proc/self/fd/%d", fd);
    ssize_t size = readlink(link, path, sizeof(path) - 1);
    if (size < 0) return;
    path[size] = 0;
    size_t prefix = strlen(root), length = strlen(path);
    if (strncmp(path, root, prefix) || path[prefix] != '/' || length < 8 ||
        strcmp(path + length - 8, "-journal")) return;
    pthread_mutex_lock(&mutex);
    for (size_t i = 0; i < count; ++i) {
        if (!strcmp(seen[i], path)) {
            pthread_mutex_unlock(&mutex);
            return;
        }
    }
    if (count == 1024) abort();
    strcpy(seen[count++], path);
    pthread_mutex_unlock(&mutex);
    fprintf(stderr, "SQLPOOL1 delayed journal fsync: %s\n", path);
    struct timespec remaining = {.tv_sec = 6, .tv_nsec = 250000000};
    while (nanosleep(&remaining, &remaining) && errno == EINTR) {}
}

int fsync(int fd) {
    int (*real_sync)(int) = dlsym(RTLD_NEXT, "fsync");
    throttle(fd);
    return real_sync(fd);
}

int fdatasync(int fd) {
    int (*real_sync)(int) = dlsym(RTLD_NEXT, "fdatasync");
    throttle(fd);
    return real_sync(fd);
}
