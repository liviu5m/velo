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

void execute(char *args[], int argsCount, char *responseBuffer) {
	
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
		int ready = poll(polls, pollId, -1); 
  
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
							execute(args, argsCount, responseBuffer);
							write(polls[i].fd, responseBuffer, strlen(responseBuffer));
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