#ifndef TYPES_H
#define TYPES_H

#include <sys/time.h>

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

extern struct keyValues keys[BUFFER_SIZE];
extern int keyCount;

extern struct listValues lists[BUFFER_SIZE];
extern int listCount;

extern struct stream streams[100];
extern int streamCount;

extern struct blockedQueue blockedQueues[BUFFER_SIZE];
extern int blockedQueuesCount;
long long get_current_time_ms();
#endif