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
#include "lists.h"
#include "types.h"
#include "streams.h"

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

void execute(char *args[], int argsCount, char *responseBuffer, int clientFd, struct clientSession *clientSession) {
	if(strcasecmp(args[0], "DISCARD") == 0) {
		if(!clientSession->isActiveMultiQueue) {
			sprintf(responseBuffer, "-ERR DISCARD without MULTI\r\n");
			return;
		}
		clientSession->isActiveMultiQueue = false;
		for(int i = 0;i<clientSession->multiQueuesCount;i++) {
			for (int j = 0; j < clientSession->multiQueues[i].argsCount; j++) {
				free(clientSession->multiQueues[i].args[j]);
			}
			clientSession->multiQueues[i].argsCount = 0;
			clientSession->multiQueues[i].clientFd = -1;
		}
		clientSession->multiQueuesCount = 0;
		sprintf(responseBuffer, "+OK\r\n");
		return;
	}else	if(strcasecmp(args[0], "EXEC") == 0) {
		if(!clientSession->isActiveMultiQueue) {
			sprintf(responseBuffer, "-ERR EXEC without MULTI\r\n");
			return;
		}
		clientSession->isActiveMultiQueue = false;
		int count = clientSession->multiQueuesCount;
		clientSession->multiQueuesCount = 0;
		sprintf(responseBuffer, "*%d\r\n", count);
		for(int i = 0;i<count;i++) {
			char tempResponse[BUFFER_SIZE];
			tempResponse[0] = '\0';
			execute(clientSession->multiQueues[i].args, clientSession->multiQueues[i].argsCount, tempResponse, clientSession->multiQueues[i].clientFd,clientSession);
			strcat(responseBuffer, tempResponse);
			for (int j = 0; j < clientSession->multiQueues[i].argsCount; j++) {
				free(clientSession->multiQueues[i].args[j]);
			}
			clientSession->multiQueues[i].argsCount = 0;
			clientSession->multiQueues[i].clientFd = -1;
		}
		return;
	}
	if(clientSession->isActiveMultiQueue && strcasecmp(args[0], "MULTI") != 0) {
		for(int i = 0;i<argsCount;i++) {
			clientSession->multiQueues[clientSession->multiQueuesCount].args[i] = strdup(args[i]);
		}
		clientSession->multiQueues[clientSession->multiQueuesCount].argsCount = argsCount;
		clientSession->multiQueues[clientSession->multiQueuesCount].clientFd = clientFd;
		clientSession->multiQueuesCount++;
		sprintf(responseBuffer, "+QUEUED\r\n");
		return;
	}
	int status = executeLists(args, argsCount, responseBuffer, clientFd);
	if(status == 1) return;
	status = executeStreams(args, argsCount, responseBuffer, clientFd);
	if(status == 1) return;
  if(argsCount > 1 && strcasecmp(args[0], "ECHO") == 0) {
    sprintf(responseBuffer, "$%zu\r\n%s\r\n", strlen(args[1]), args[1]);
  }else if(strcasecmp(args[0], "SET") == 0) {
		keys[keyCount].key = strdup(args[1]);
		keys[keyCount].value = strdup(args[2]);
		if(argsCount == 3) keys[keyCount].expireAt = 0;
		else {
			if(strcasecmp(args[3], "EX") == 0) keys[keyCount].expireAt = get_current_time_ms()+atoi(args[4])*1000;
			else if(strcasecmp(args[3], "PX") == 0) keys[keyCount].expireAt = get_current_time_ms()+atoi(args[4]);
		}
		keyCount++;
		sprintf(responseBuffer, "+OK\r\n");
	}else if(strcasecmp(args[0], "GET") == 0) {
		for(int i = 0;i<keyCount;i++) {
			if(strcmp(keys[i].key, args[1]) == 0 && (keys[i].expireAt == 0 || keys[i].expireAt > get_current_time_ms())) {
				sprintf(responseBuffer, "$%zu\r\n%s\r\n", strlen(keys[i].value), keys[i].value);
				return;
			}
		}
		sprintf(responseBuffer, "$-1\r\n");
	}else if(strcasecmp(args[0], "TYPE") == 0) {
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
	}else if(strcasecmp(args[0], "INCR") == 0) {
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
	}else if(strcasecmp(args[0], "MULTI") == 0) {
		if(clientSession->isActiveMultiQueue) {
			sprintf(responseBuffer, "-ERR MULTI calls can not be nested\r\n");
			return;
		}
		clientSession->multiQueues[clientSession->multiQueuesCount].clientFd = clientFd;
		clientSession->multiQueues[clientSession->multiQueuesCount].argsCount = 0;
		clientSession->isActiveMultiQueue = true;
		sprintf(responseBuffer, "+OK\r\n");
	}else {
    strcpy(responseBuffer, "+PONG\r\n");
  }
}

int main() {

  setbuf(stdout, NULL);
	setbuf(stderr, NULL);
  int clientAddr;
  int serverFd = socket(AF_INET, SOCK_STREAM, 0);
  if (serverFd == -1) {
		printf("Socket creation failed: %s...\n", strerror(errno));
		return 1;
	}

  int reuse = 1;
	if (setsockopt(serverFd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
		printf("SO_REUSEADDR failed: %s \n", strerror(errno));
		return 1;
	}

  struct sockaddr_in servAddr;
  servAddr.sin_family = AF_INET;
  servAddr.sin_port = htons(6379);
  servAddr.sin_addr.s_addr = htonl(INADDR_ANY);
	
	if (bind(serverFd, (struct sockaddr *) &servAddr, sizeof(servAddr)) != 0) {
		printf("Bind failed: %s \n", strerror(errno));
		return 1;
	}

  int connection_backlog = 5;
	if (listen(serverFd, connection_backlog) != 0) {
		printf("Listen failed: %s \n", strerror(errno));
		return 1;
	}
	polls[0].fd = serverFd;
	polls[0].events = POLLIN;
	clientSessions[0].isActiveMultiQueue = false;
	printf("Waiting for a client to connect...\n");
	while(1) {
		int ready = poll(polls, pollId, 10); 
		checkBlop();
		checkBlockStreams();
		if (ready == -1) {
			perror("poll failed");
			break;
		}
		for(int i = 0;i<pollId;i++) {
			if (polls[i].revents & POLLIN) {
				if(polls[i].fd == serverFd) {
					int clientAddrLen = sizeof(clientAddr);
					int clientFd = accept(serverFd, (struct sockaddr *) &clientAddr, &clientAddrLen);
					if(clientFd == -1) {
						printf("Client failed to connect\n");
						return 0;
					}
					polls[pollId].fd = clientFd;
					polls[pollId].events = POLLIN;
					clientSessions[pollId].isActiveMultiQueue = false;
					clientSessions[pollId].multiQueuesCount = 0;
					pollId++;
					
					printf("Client connected\n");
				} else {
					char buffer[BUFFER_SIZE];
					int bytes = read(polls[i].fd, buffer, BUFFER_SIZE-1);
					if(bytes <= 0) {
						printf("Client disconnected \n");
						close(polls[i].fd);
						polls[i] = polls[pollId - 1];
						clientSessions[i] = clientSessions[pollId - 1];
						pollId--;
						i--;
					}else {
						buffer[bytes] = '\0';
						char *args[1024];
						int argsCount = parseRespRequest(buffer, args);
						if (argsCount > 0) {
							char responseBuffer[BUFFER_SIZE]; 
							responseBuffer[0] = '\0';
							execute(args, argsCount, responseBuffer,polls[i].fd, &clientSessions[i]);
							if(responseBuffer[0] != '\0') {
								write(polls[i].fd, responseBuffer, strlen(responseBuffer));								
							}
							for(int j = 0; j < argsCount; j++) {
								free(args[j]);
							}
						}
					}
				}
			}
		}
	}
	
	close(serverFd);

  return 0;
}