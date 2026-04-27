#ifndef TYPES_H
#define TYPES_H

#include <sys/time.h>
#include <stdbool.h>

#define BUFFER_SIZE 2048
struct keyValues {
  char *key;
  char *value;
  long long expireAt;
};

struct listValues {
  char *key;
  char *values[BUFFER_SIZE];
  int valuesCount;
};

struct blockedQueue {
  int clientFd;
  char *key;
  long long expireAt;
};

struct pair {
  char key[BUFFER_SIZE];
  char value[BUFFER_SIZE];
};

struct entries {
  char id[64];
  struct pair pairs[16];
  int pairsCount;
};

struct stream {
  char key[128];
  struct entries entries[64];
  int entriesCount;
};

struct blockStream {
	int clientFd;
	long long miliseconds;
	int pairsCount;
	struct pair pairs[16];
};
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
};


extern struct keyValues keys[BUFFER_SIZE];
extern int keyCount;

extern struct listValues lists[BUFFER_SIZE];
extern int listCount;

extern struct stream streams[100];
extern int streamCount;

extern struct blockedQueue blockedQueues[BUFFER_SIZE];
extern int blockedQueuesCount;

extern int blocksStreamCount;
extern struct blockStream blocksStream[100];

extern struct multiQueue multiQueues[128];
extern int multiQueuesCount;
extern bool isActiveMultiQueue;


extern struct pollfd polls[1024];
extern struct clientSession clientSessions[1024];
extern int pollId;

long long get_current_time_ms();
#endif