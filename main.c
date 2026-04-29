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

int port = 6379;
char *serverRole = "master";
char *masterReplicationId = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
char *masterReplicationOffset = "0";
char *masterHost;
char *masterPort;
int replicaFds[100];
int replicaCount = 0;

void execute(char *args[], int argsCount, char *responseBuffer, int clientFd, struct clientSession *clientSession) {
	int status = 0;
	if(strcasecmp(args[0], "UNWATCH") == 0) 
		status = unwatchFunc(responseBuffer, clientSession);

	else if(strcasecmp(args[0], "WATCH") == 0) 
		status = watch(args, argsCount, responseBuffer, clientSession);	

	else if(strcasecmp(args[0], "DISCARD") == 0) 
		status = discardFunc(responseBuffer, clientSession);	

	else	if(strcasecmp(args[0], "EXEC") == 0) 
		status = exec(responseBuffer, clientSession);

	else if(clientSession->isActiveMultiQueue && strcasecmp(args[0], "MULTI") != 0)
		status = multiQueue(args, argsCount, responseBuffer, clientSession, clientFd);
	
	if(status == 1) return;

	status = executeLists(args, argsCount, responseBuffer, clientFd);
	if(status == 1) return;
	status = executeStreams(args, argsCount, responseBuffer, clientFd);
	if(status == 1) return;

  if(argsCount > 1 && strcasecmp(args[0], "ECHO") == 0) 
		echo(args, argsCount, responseBuffer);

  else if(strcasecmp(args[0], "SET") == 0) {
		set(args, argsCount, responseBuffer, clientSession);
		if (strcmp(serverRole, "master") == 0) {
      propagate(args, argsCount, replicaFds, replicaCount);
    }
	}

	else if(strcasecmp(args[0], "GET") == 0) 
		get(args, argsCount, responseBuffer, clientSession);	

	else if(strcasecmp(args[0], "TYPE") == 0) 
		type(args, argsCount, responseBuffer, clientSession);	

	else if(strcasecmp(args[0], "INCR") == 0) 
		incr(args, argsCount, responseBuffer, clientSession);

	else if(strcasecmp(args[0], "MULTI") == 0) 
		multi(responseBuffer, clientSession, clientFd);

	else if(strcasecmp(args[0], "INFO") == 0) 
		info(serverRole, masterReplicationId, masterReplicationOffset, responseBuffer);

	else if(strcasecmp(args[0], "REPLCONF") == 0) 
		replconf(responseBuffer);

	else if(strcasecmp(args[0],"PSYNC") == 0) 
		psync(responseBuffer, masterReplicationId, clientFd, replicaFds, &replicaCount);

	else 
		pong(responseBuffer);
  
}

int main(int argc, char *argv[]) {

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
	for(int i = 0;i<argc;i++) {
		if(strcmp(argv[i], "--port") == 0) {
			port = atoi(argv[i+1]);
			i++;
		}else if(strcmp(argv[i], "--replicaof") == 0) {
			serverRole = "slave";
			char *hostPort = argv[i+1];
			char *colon = strchr(hostPort, ' ');
			if(colon) {
				*colon = '\0';
				masterHost = hostPort;
				masterPort = colon + 1;
			}
			i++;
		}
	}
	printf("Starting server on port %d...\n", port);
  struct sockaddr_in servAddr;
  servAddr.sin_family = AF_INET;
  servAddr.sin_port = htons(port);
  servAddr.sin_addr.s_addr = htonl(INADDR_ANY);
	
	if (bind(serverFd, (struct sockaddr *) &servAddr, sizeof(servAddr)) != 0) {
		printf("Bind failed: %s \n", strerror(errno));
		return 1;
	}

	if (strcmp(serverRole, "slave") == 0) {
		printf("Hello, I am a replica. Connecting to master at %s:%s...\n", masterHost, masterPort);
    int masterFd = socket(AF_INET, SOCK_STREAM, 0);
    
    struct sockaddr_in master_addr;
    master_addr.sin_family = AF_INET;
    master_addr.sin_port = htons(atoi(masterPort)); 
    inet_pton(AF_INET, "127.0.0.1", &master_addr.sin_addr);

    if (connect(masterFd, (struct sockaddr *)&master_addr, sizeof(master_addr)) == 0) {
			printf("Connected to master. Sending PING...\n");
			char *ping = "*1\r\n$4\r\nPING\r\n";
			char replConfigPort[256];
			int lenPort = 0, tempPort = port;
			while(tempPort) {
				lenPort++;
				tempPort /= 10;
			}
			char responseBuffer[BUFFER_SIZE];
			sprintf(replConfigPort, "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$%d\r\n%d\r\n", lenPort, port);
			char *replCapaSync = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
			send(masterFd, ping, strlen(ping), 0);
			recv(masterFd, responseBuffer, sizeof(responseBuffer), 0);
			send(masterFd, replConfigPort, strlen(replConfigPort), 0);
			recv(masterFd, responseBuffer, sizeof(responseBuffer), 0); 
			send(masterFd, replCapaSync, strlen(replCapaSync), 0);
			recv(masterFd, responseBuffer, sizeof(responseBuffer), 0); 
			char *token = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
			send(masterFd, token, strlen(token), 0);
			recv(masterFd, responseBuffer, sizeof(responseBuffer), 0);
			polls[pollId].fd = masterFd;
			polls[pollId].events = POLLIN;
			clientSessions[pollId].isActiveMultiQueue = false;
			clientSessions[pollId].multiQueuesCount = 0;
			clientSessions[pollId].watchedKeysCount = 0;
			clientSessions[pollId].isKeyChanged = false;
			clientSessions[pollId].masterFd = masterFd;
			pollId++;
    } else {
			printf("Connection to master failed: %s\n", strerror(errno));
    }
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
					clientSessions[pollId].watchedKeysCount = 0;
					clientSessions[pollId].isKeyChanged = false;
					clientSessions[pollId].masterFd = -1;
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
						char *currentPos = buffer;

            while (currentPos != NULL && *currentPos != '\0') {
              char *args[1024];
              
              int argsCount = parseRespRequest(currentPos, args);
              
              if (argsCount > 0) {
                char responseBuffer[BUFFER_SIZE];
                responseBuffer[0] = '\0';
                
                execute(args, argsCount, responseBuffer, polls[i].fd, &clientSessions[i]);
                
                if (responseBuffer[0] != '\0' && clientSessions[i].masterFd == polls[i].fd) {
                  write(polls[i].fd, responseBuffer, strlen(responseBuffer));
                }

                for (int j = 0; j < argsCount; j++) {
                  free(args[j]);
                }
              }

              currentPos = strchr(currentPos + 1, '*');
            }
					}
				}
			}
		}
	}
	
	close(serverFd);

  return 0;
}