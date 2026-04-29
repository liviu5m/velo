#include <stdio.h>
#include <stdlib.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netinet/ip.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <poll.h>
#include <sys/time.h>
#include <stdbool.h>

#define BUFFER_SIZE 2048



struct pollfd polls[1024];
int pollId = 1;

struct keyValues {
	char *key;
	char *value;
	long long expireAt;
};
int keyCount = 0;
struct keyValues keys[BUFFER_SIZE];

struct listValues {
	char *key;
	char *values[BUFFER_SIZE];
	int valuesCount;
};
int listCount = 0;
struct listValues lists[BUFFER_SIZE];

struct blockedQueue {
	int clientFd;
	char *key;
	long long expireAt;
};
int blockedQueuesCount = 0;
struct blockedQueue blockedQueues[BUFFER_SIZE];

struct pair {
	char key[BUFFER_SIZE];
	char value[BUFFER_SIZE];
};



struct entries {
	char id[64];
	struct pair pairs[16];
	int pairCount;
};

struct stream {
	char key[128];
	struct entries entries[64];
	int entriesCount;
};
int streamCount = 0;
struct stream streams[100];

struct blockStream {
	int clientFd;
	long long miliseconds;
	int pairsCount;
	struct pair pairs[16];
};
int blocksStreamCount = 0;
struct blockStream blocksStream[100];


struct multiQueue {
	int clientFd;
	char *args[128];
	int argsCount;
};

struct clientSession {
	bool isActiveMultiQueue;
	struct multiQueue multiQueues[128];
	int multiQueuesCount;
	bool isKeyChanged;
	char *watchedKeys[128];
	int watchedKeysCount;
	int masterFd;
};
struct clientSession clientSessions[1024];

long long get_current_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)(tv.tv_sec) * 1000 + (tv.tv_usec / 1000);
}
