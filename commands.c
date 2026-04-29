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
#include <arpa/inet.h>
#include "lists.h"
#include "types.h"
#include "streams.h"
#include "commands.h"

#define BUFFER_SIZE 2048

int parseRespRequest(char *buffer, char *args[]) {
	int argsCount = 0;
	if (buffer[0] != '*') return 0;

	char *curr = strstr(buffer, "\r\n");
	if (!curr) return 0;
	curr += 2;

	int totalArgs = atoi(&buffer[1]);

	while (argsCount < totalArgs && curr != NULL) {
		if (curr[0] == '$') {
			int argsSize = atoi(&curr[1]);
			
			curr = strstr(curr, "\r\n");
			if (!curr) break;
			curr += 2;

			args[argsCount++] = strndup(curr, argsSize);

			curr = strstr(curr, "\r\n");
			if (curr) curr += 2;
		} else {
			break;
		}
	}
	return argsCount;
}

void notifiyKeyChange(char *key, struct clientSession *currentClient) {
	for(int i = 0;i<pollId;i++) {
		struct clientSession *clientSession = &clientSessions[i];
		if(currentClient == clientSession) continue;
		for(int j = 0;j<clientSession->watchedKeysCount;j++) {
			if(strcmp(clientSession->watchedKeys[j], key) == 0) {
				clientSession->isKeyChanged = true;
				break;
			}
		}
	}
}

void unwatch(struct clientSession *clientSession) {
	for(int i = 0;i<clientSession->watchedKeysCount;i++) {
		free(clientSession->watchedKeys[i]);
	}
	clientSession->watchedKeysCount = 0;
	clientSession->isKeyChanged = false;
}

void discard(struct clientSession *clientSession) {
	clientSession->isActiveMultiQueue = false;
	for(int i = 0;i<clientSession->multiQueuesCount;i++) {
		for (int j = 0; j < clientSession->multiQueues[i].argsCount; j++) {
			free(clientSession->multiQueues[i].args[j]);
		}
		clientSession->multiQueues[i].argsCount = 0;
		clientSession->multiQueues[i].clientFd = -1;
	}
	clientSession->multiQueuesCount = 0;
}

int unwatchFunc(char *responseBuffer, struct clientSession *clientSession) {
  unwatch(clientSession);
	sprintf(responseBuffer, "+OK\r\n");
	return 1;
}

int watch(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession) {
  if(clientSession->isActiveMultiQueue) {
    sprintf(responseBuffer, "-ERR WATCH inside MULTI is not allowed\r\n");
    return 1;
  }
  for(int i = 1;i<argsCount;i++) {
    clientSession->watchedKeys[clientSession->watchedKeysCount++] = strdup(args[i]);
  }
  sprintf(responseBuffer, "+OK\r\n");
  return 1;
}

int discardFunc(char *responseBuffer, struct clientSession *clientSession) {
  if(!clientSession->isActiveMultiQueue) {
    sprintf(responseBuffer, "-ERR DISCARD without MULTI\r\n");
    unwatch(clientSession);
    return 1;
  }
  discard(clientSession);
  sprintf(responseBuffer, "+OK\r\n");
  unwatch(clientSession);
  return 1;
}

int exec(char *responseBuffer, struct clientSession *clientSession) {
  if(!clientSession->isActiveMultiQueue) {
    sprintf(responseBuffer, "-ERR EXEC without MULTI\r\n");
    unwatch(clientSession);
    discard(clientSession);
    return 1;
  }
  if(clientSession->isKeyChanged) {
    sprintf(responseBuffer, "*-1\r\n");
    clientSession->isActiveMultiQueue = false;
    clientSession->isKeyChanged = false;
    unwatch(clientSession);
    discard(clientSession);
    return 1;
  }
  clientSession->isActiveMultiQueue = false;
  int count = clientSession->multiQueuesCount;
  clientSession->multiQueuesCount = 0;
  sprintf(responseBuffer, "*%d\r\n", count);
  for(int i = 0;i<count;i++) {
    char tempResponse[BUFFER_SIZE];
    tempResponse[0] = '\0';
    printf("Executing command from multi queue: %s \n", clientSession->multiQueues[i].args[0]);
    execute(clientSession->multiQueues[i].args, clientSession->multiQueues[i].argsCount, tempResponse, clientSession->multiQueues[i].clientFd,clientSession);
    strcat(responseBuffer, tempResponse);
    for (int j = 0; j < clientSession->multiQueues[i].argsCount; j++) {
      free(clientSession->multiQueues[i].args[j]);
    }
    clientSession->multiQueues[i].argsCount = 0;
    clientSession->multiQueues[i].clientFd = -1;
  }
  unwatch(clientSession);
  return 1;
}

int multiQueue(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession, int clientFd) {
  for(int i = 0;i<argsCount;i++) {
    clientSession->multiQueues[clientSession->multiQueuesCount].args[i] = strdup(args[i]);
  }
  clientSession->multiQueues[clientSession->multiQueuesCount].argsCount = argsCount;
  clientSession->multiQueues[clientSession->multiQueuesCount].clientFd = clientFd;
  clientSession->multiQueuesCount++;
  sprintf(responseBuffer, "+QUEUED\r\n");
  return 1;
}

void echo(char *args[], int argsCount, char *responseBuffer) {
  if(argsCount > 1) {
    sprintf(responseBuffer, "$%zu\r\n%s\r\n", strlen(args[1]), args[1]);
  }else {
    sprintf(responseBuffer, "$-1\r\n");
  }
}

void set(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession) {
  notifiyKeyChange(args[1], clientSession);
  int foundId = -1;
  for(int i = 0;i<keyCount;i++) {
    if(strcmp(args[1], keys[i].key) == 0) {
      foundId = i;
      break;
    }
  }
  if(foundId == -1) foundId = keyCount;
  keys[foundId].key = strdup(args[1]);
  keys[foundId].value = strdup(args[2]);
  if(argsCount == 3) keys[foundId].expireAt = 0;
  else {
    if(strcasecmp(args[3], "EX") == 0) keys[foundId].expireAt = get_current_time_ms()+atoi(args[4])*1000;
    else if(strcasecmp(args[3], "PX") == 0) keys[foundId].expireAt = get_current_time_ms()+atoi(args[4]);
  }
  if(foundId == keyCount) keyCount++;
  sprintf(responseBuffer, "+OK\r\n");
}

void get(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession) {
  for(int i = 0;i<keyCount;i++) {
    if(strcmp(keys[i].key, args[1]) == 0 && (keys[i].expireAt == 0 || keys[i].expireAt > get_current_time_ms())) {
      sprintf(responseBuffer, "$%zu\r\n%s\r\n", strlen(keys[i].value), keys[i].value);
      return;
    }
  }
  sprintf(responseBuffer, "$-1\r\n");
}

void type(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession) {
  for(int i = 0;i<keyCount;i++) {
    if(strcmp(args[1], keys[i].key) == 0) {
      sprintf(responseBuffer, "+string\r\n");
      return;
    }
  }
  for(int i = 0;i<streamCount;i++) {
    if(strcmp(args[1], streams[i].key) == 0) {
      sprintf(responseBuffer, "+stream\r\n");
      return;
    }
  }
  sprintf(responseBuffer, "+none\r\n");
}

void incr(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession) {
  notifiyKeyChange(args[1], clientSession);
  for(int i = 0;i<keyCount;i++) {
    printf("Checking key: %s\n", keys[i].key);
    if(strcmp(args[1], keys[i].key) == 0 && (keys[i].expireAt == 0 || keys[i].expireAt > get_current_time_ms())) {
      if(atoll(keys[i].value) != 0) {
        long long value = atoll(keys[i].value);
        value++;
        sprintf(keys[i].value, "%lld", value);
        sprintf(responseBuffer, ":%lld\r\n", value);
        return;
      }else {
        sprintf(responseBuffer, "-ERR value is not an integer or out of range\r\n");
        return;
      }
    }
  }
  keys[keyCount].key = strdup(args[1]);
  keys[keyCount].value = strdup("1");	
  keys[keyCount].expireAt = 0;
  keyCount++;
  sprintf(responseBuffer, ":1\r\n");
}

void multi(char *responseBuffer, struct clientSession *clientSession, int clientFd) {
  if(clientSession->isActiveMultiQueue) {
    sprintf(responseBuffer, "-ERR MULTI calls can not be nested\r\n");
    return;
  }
  clientSession->multiQueues[clientSession->multiQueuesCount].clientFd = clientFd;
  clientSession->multiQueues[clientSession->multiQueuesCount].argsCount = 0;
  clientSession->isActiveMultiQueue = true;
  sprintf(responseBuffer, "+OK\r\n");
}

void info(char *serverRole, char *masterReplicationId, char *masterReplicationOffset, char *responseBuffer) {
  char el[256];
  sprintf(el, "role:%s\r\nmaster_replid:%s\r\nmaster_repl_offset:%s\r\n", serverRole, masterReplicationId, masterReplicationOffset);
  printf("INFO response: %s", el);
  sprintf(responseBuffer,"$%zu\r\n%s\r\n", strlen(el), el);
}

void replconf(char *responseBuffer) {
  strcpy(responseBuffer, "+OK\r\n");
}

void psync(char *responseBuffer, char *masterReplicationId, int clientFd, int *replicaFds, int *replicaCount) {
  replicaFds[(*replicaCount)++] = clientFd;
  sprintf(responseBuffer, "+FULLRESYNC %s 0\r\n", masterReplicationId);
  send(clientFd, responseBuffer, strlen(responseBuffer), 0);
  const char empty_rdb_hex[] = {
    0x52, 0x45, 0x44, 0x49, 0x53, 0x30, 0x30, 0x31, 0x31, 0xfa, 0x09, 0x72, 0x65, 0x64, 0x69, 0x73, 
    0x2d, 0x76, 0x65, 0x72, 0x05, 0x37, 0x2e, 0x32, 0x2e, 0x30, 0xfa, 0x0a, 0x72, 0x65, 0x64, 0x69, 
    0x73, 0x2d, 0x62, 0x69, 0x74, 0x73, 0xc0, 0x40, 0xfa, 0x05, 0x63, 0x74, 0x69, 0x6d, 0x65, 0xc2, 
    0x6d, 0x08, 0xbc, 0x65, 0xfa, 0x08, 0xd7, 0x73, 0x65, 0x2d, 0x6d, 0x65, 0x6d, 0xc2, 0xb0, 0xc4, 
    0x10, 0x00, 0xfa, 0x0c, 0x61, 0x6f, 0x66, 0x2d, 0x62, 0x61, 0x73, 0x65, 0xc0, 0x00, 0xff, 0x8b, 
    0xe6, 0xff, 0x42, 0x8c, 0x0e, 0x33, 0x4a
  };
  int rdb_size = sizeof(empty_rdb_hex);

  char *rdb_header = "$88\r\n";
  send(clientFd, rdb_header, strlen(rdb_header), 0);
  send(clientFd, empty_rdb_hex, 88, 0);
  responseBuffer[0] = '\0';
}

void pong(char *responseBuffer) {
  sprintf(responseBuffer, "+PONG\r\n");
}

void propagate(char *args[], int argsCount, int *replicaFds, int replicaCount) {
  char buffer[BUFFER_SIZE];
  int offset = 0;
  offset += sprintf(buffer + offset, "*%d\r\n", argsCount);
  for(int i = 0;i<argsCount;i++) {
    offset += sprintf(buffer + offset, "$%zu\r\n%s\r\n", strlen(args[i]), args[i]);
  }
  printf("Propagating command to replicas: %s", buffer);
  for(int i = 0;i<replicaCount;i++) {
    printf("Sending to replica fd %d\n", replicaFds[i]);
    send(replicaFds[i], buffer, strlen(buffer), 0);
  }
}