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

int parseRespRequest(char *buffer, char * args[]) {
	int argsCount = 0;
	char *argsValue;
	int argsSize = 0;
	if(buffer[0] == '*') {
		char *curr = strstr(buffer, "\r\n");
		int count = atoi(&buffer[1]);
		while(argsCount < count && curr != NULL) {
			curr+=2;
			if(curr[0] == '$') {
				argsSize = atoi(&curr[1]);
			}else {
				args[argsCount++] = strndup(curr, argsSize);
			}
			curr = strstr(curr, "\r\n");
			
		}
	}
	return argsCount;
}

long long get_current_time_ms() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (long long)(tv.tv_sec) * 1000 + (tv.tv_usec / 1000);
}

void checkBlop() {
	for(int i = 0;i<blockedQueuesCount;i++) {
		if(blockedQueues[i].expireAt > 0 && get_current_time_ms() >= blockedQueues[i].expireAt) {
        write(blockedQueues[i].clientFd, "*-1\r\n", 5);
        free(blockedQueues[i].key);
        for(int k = i; k < blockedQueuesCount - 1; k++) {
            blockedQueues[k] = blockedQueues[k+1];
        }
        blockedQueuesCount--;
        i--; 
        continue;
    }
		for(int j = 0;j<listCount;j++) {
			if(strcmp(lists[j].key, blockedQueues[i].key) == 0 && lists[j].valuesCount > 0) {
				
				char responseBuffer[BUFFER_SIZE];
				sprintf(responseBuffer, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", strlen(lists[j].key), lists[j].key, strlen(lists[j].values[0]), lists[j].values[0]);
				for(int t = 1;t<lists[j].valuesCount;t++) {
					lists[j].values[t-1] = lists[j].values[t];
				}
				lists[j].valuesCount--;
				lists[j].values[lists[j].valuesCount] = NULL;
				write(blockedQueues[i].clientFd, responseBuffer, strlen(responseBuffer));								
				free(blockedQueues[i].key);
        for(int k = i; k < blockedQueuesCount - 1; k++) {
            blockedQueues[k] = blockedQueues[k+1];
        }
        blockedQueuesCount--;
        i--;
				break;
			}
		}
	}
}


void execute(char *args[], int argsCount, char *responseBuffer, int clientFd) {
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
	}else if(strcasecmp(args[0], "RPUSH") == 0) {
		for(int i = 0;i<listCount;i++) {
			if(strcmp(lists[i].key, args[1]) == 0) {
				for(int j = 2;j<argsCount;j++) {
					lists[i].values[lists[i].valuesCount] = strdup(args[j]);
					lists[i].valuesCount++;
				}
				sprintf(responseBuffer, ":%i\r\n", lists[i].valuesCount);
				checkBlop();
				return;
			}
		}
		lists[listCount].key = strdup(args[1]);
		for(int j = 2;j<argsCount;j++) {
			lists[listCount].values[lists[listCount].valuesCount] = strdup(args[j]);
			lists[listCount].valuesCount++;
		}
		listCount++;
		sprintf(responseBuffer, ":%i\r\n", lists[listCount-1].valuesCount);
		checkBlop();
	}else if(strcasecmp(args[0], "LPUSH") == 0) {
		for(int i = 0;i<listCount;i++) {
			if(strcmp(lists[i].key, args[1]) == 0) {
				int elsNr = argsCount-2;
				for(int j = lists[i].valuesCount-1; j>=0;j--) {
					lists[i].values[j+elsNr] = lists[i].values[j];
				}
				lists[i].valuesCount += elsNr;
				for(int j = 0;j<elsNr;j++) {
					lists[i].values[j] = strdup(args[argsCount-j-1]);
				}
				sprintf(responseBuffer, ":%i\r\n", lists[i].valuesCount);
				checkBlop();
				return;
			}
		}
		lists[listCount].key = strdup(args[1]);
		int elsNr = argsCount-2;
		for(int j = 0;j<elsNr;j++) {
			lists[listCount].values[j] = strdup(args[argsCount-j-1]);
		}
		lists[listCount].valuesCount += elsNr;
		listCount++;
		sprintf(responseBuffer, ":%i\r\n", lists[listCount-1].valuesCount);
		checkBlop();
	}else if(strcasecmp(args[0], "LLEN") == 0) {
		for(int i = 0;i<listCount;i++) {
			if(strcmp(lists[i].key, args[1]) == 0) {
				sprintf(responseBuffer,":%i\r\n", lists[i].valuesCount);
				return;
			}
		}
		sprintf(responseBuffer,":0\r\n");
	}else if(strcasecmp(args[0], "LPOP") == 0) {
		for(int i = 0;i<listCount;i++) {
			if(strcmp(lists[i].key, args[1]) == 0) {
				int count = 1;
				if(argsCount == 3) count = atoi(args[2]);
				if(count != 1) sprintf(responseBuffer, "*%i\r\n", count);
				printf("%s\n", responseBuffer);
				for(int j = 0;j<count;j++) {
					char el[BUFFER_SIZE];
					sprintf(el,"$%zu\r\n%s\r\n",strlen(lists[i].values[0]), lists[i].values[0]);
					strcat(responseBuffer, el);
				printf("%s\n", responseBuffer);
					for(int t = 1;t<lists[i].valuesCount;t++) {
						lists[i].values[t-1] = lists[i].values[t];
					}
					lists[i].valuesCount--;
					lists[i].values[lists[i].valuesCount] = NULL;
				}
				printf("%s\n", responseBuffer);
				
				return;
			}
		}
		sprintf(responseBuffer,"$-1\r\n");
	}else if(strcasecmp(args[0], "BLPOP") == 0) {
		for(int j = 0;j<listCount;j++) {
			if(strcmp(lists[j].key, args[1]) == 0) {
				sprintf(responseBuffer, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", strlen(lists[j].key), lists[j].key, strlen(lists[j].values[0]), lists[j].values[0]);
				for(int t = 1;t<lists[j].valuesCount;t++) {
					lists[j].values[t-1] = lists[j].values[t];
				}
				lists[j].valuesCount--;
				lists[j].values[lists[j].valuesCount] = NULL;
				
				return;
			}
		}
		blockedQueues[blockedQueuesCount].key = strdup(args[1]);
		blockedQueues[blockedQueuesCount].clientFd = clientFd;
		double timeoutSec = atof(args[2]); 
    if(timeoutSec > 0) {
        blockedQueues[blockedQueuesCount].expireAt = get_current_time_ms() + (long long)(timeoutSec * 1000);
    } else {
        blockedQueues[blockedQueuesCount].expireAt = 0; 
    }
		blockedQueuesCount++;
		responseBuffer[0] = '\0';
		return;
	}else if(strcasecmp(args[0], "LRANGE") == 0) {
		for(int i = 0;i<listCount;i++) {
			if(strcmp(lists[i].key, args[1]) == 0) {
				int startIndex = atoi(args[2]), finalIndex = atoi(args[3]);
				int count = lists[i].valuesCount;
				if(startIndex < 0) {
					startIndex += count;
					if(startIndex < 0) startIndex = 0;
				}
				if(finalIndex < 0) {
					finalIndex += count;
					if(finalIndex < 0) finalIndex = 0;
				}
				if(count <= finalIndex) finalIndex = count-1;
				int numsCount = finalIndex-startIndex + 1;
				if(numsCount < 0) numsCount = 0;
				sprintf(responseBuffer, "*%i\r\n", numsCount);
				for(int j = startIndex;j<=finalIndex;j++) {
					char el[BUFFER_SIZE];
					sprintf(el, "$%zu\r\n%s\r\n" ,strlen(lists[i].values[j]), lists[i].values[j]);
					strcat(responseBuffer, el);
				}
				return;
			}
		}
		sprintf(responseBuffer, "*0\r\n");
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
	printf("Waiting for a client to connect...\n");
	while(1) {
		int ready = poll(polls, pollId, 10); 
		checkBlop();
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
					pollId++;
					
					printf("Client connected\n");
				} else {
					char buffer[BUFFER_SIZE];
					int bytes = read(polls[i].fd, buffer, BUFFER_SIZE-1);
					if(bytes <= 0) {
						printf("Client disconnected \n");
						close(polls[i].fd);
						polls[i] = polls[pollId - 1];
						pollId--;
						i--;
					}else {
						buffer[bytes] = '\0';
						char *args[1024];
						int argsCount = parseRespRequest(buffer, args);
						if (argsCount > 0) {
							char responseBuffer[BUFFER_SIZE]; 
							responseBuffer[0] = '\0';
							execute(args, argsCount, responseBuffer,polls[i].fd);
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