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

#define BUFFER_SIZE 2048

struct pollfd polls[1024];
int pollId = 1;

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

void checkBlockStreams() {
	
	for(int b = 0;b<blocksStreamCount;b++) {
		char responseBuffer[BUFFER_SIZE];
		responseBuffer[0] = '\0';
		int totalMatchesFound = 0;
		int k = blocksStream[b].pairsCount;
		char token[2074];
		sprintf(token, "*%i\r\n", k);
		strcat(responseBuffer, token);
		for(int i = 0;i<k;i++) {
			strcat(responseBuffer, "*2\r\n");
			sprintf(token, "$%zu\r\n%s\r\n", strlen(blocksStream[b].pairs[i].key), blocksStream[b].pairs[i].key);
			strcat(responseBuffer, token);
			char end[4096];
			end[0] = '\0';
			int matchCount = 0;
			long long unsigned int mili = 0, sq = 0;
			sscanf(blocksStream[b].pairs[i].value, "%llu-%llu", &mili, &sq);
			for(int s = 0;s<streamCount;s++) {
				if(strcmp(streams[s].key, blocksStream[b].pairs[i].key) == 0) {
					for(int j = 0;j<streams[s].entriesCount;j++) {
						long long unsigned int entryMili, entrySq;
						sscanf(streams[s].entries[j].id, "%llu-%llu", &entryMili, &entrySq);
						if(entryMili > mili || (entryMili == mili && entrySq > sq)) {
							matchCount++;
							strcat(end, "*2\r\n");
							struct entries entry = streams[s].entries[j];
							sprintf(token, "$%zu\r\n%s\r\n", strlen(entry.id), entry.id);
							strcat(end, token);
							sprintf(token, "*%i\r\n", entry.pairsCount*2);
							strcat(end, token);
							for(int p = 0;p<entry.pairsCount;p++) {
								sprintf(token, "$%zu\r\n%s\r\n", strlen(entry.pairs[p].key), entry.pairs[p].key);
								strcat(end, token);
								sprintf(token, "$%zu\r\n%s\r\n", strlen(entry.pairs[p].value), entry.pairs[p].value);
								strcat(end, token);
							}
						}
					}
				}
			}
			if(matchCount > 0) totalMatchesFound++;
			sprintf(token, "*%i\r\n", matchCount);
			strcat(responseBuffer, token);
			strcat(responseBuffer, end);
		}
		if (totalMatchesFound > 0) {
      write(blocksStream[b].clientFd, responseBuffer, strlen(responseBuffer));
      close(blocksStream[b].clientFd);
      blocksStream[b] = blocksStream[blocksStreamCount-1];
      blocksStreamCount--;
      b--;
      continue;
    }

    long long now = get_current_time_ms();
    if(blocksStream[b].miliseconds != 0 && now >= blocksStream[b].miliseconds) {
      write(blocksStream[b].clientFd, "*-1\r\n", 5);
      close(blocksStream[b].clientFd);
      blocksStream[b] = blocksStream[blocksStreamCount-1];
      blocksStreamCount--;
      b--;
    }
	}
}


void execute(char *args[], int argsCount, char *responseBuffer, int clientFd) {
	int status = executeLists(args, argsCount, responseBuffer, clientFd);
	if(status == 1) return;
	printf("%i\n", status);
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
	}else if(strcasecmp(args[0], "XADD") == 0) {
		int streamIndex = -1;
		
		for (int i = 0; i < streamCount; i++) {
				if (strcmp(args[1], streams[i].key) == 0) {
						streamIndex = i;
						break;
				}
		}
		char miliTimeString[32], sqNrString[32];
		long long unsigned int miliTime = 0, sqNr = 0;
		long long unsigned int miliTimeCheck, sqNrCheck;
		bool sqNrGenerating = false, miliTimeGenerating = false;
		
		sscanf(args[2], "%[^-]-%s", miliTimeString ,sqNrString);
		miliTime = strtoull(miliTimeString, NULL, 10);

		if(strcmp(args[2], "*") == 0) {
			miliTimeGenerating = true;
			sqNrGenerating = true;
		}

		if(strcmp(sqNrString, "*") == 0) {
			sqNrGenerating = true;
			sqNr = (miliTime == 0) ? 1 : 0;
		}else sqNr = strtoull(sqNrString, NULL, 10);

		if(miliTime == 0 && sqNr == 0 && !sqNrGenerating && !miliTimeGenerating) {
			strcpy(responseBuffer, "-ERR The ID specified in XADD must be greater than 0-0\r\n");
			return;
		}
		if(streamIndex != -1) {
			sscanf(streams[streamIndex].entries[streams[streamIndex].entriesCount - 1].id, "%llu-%llu", &miliTimeCheck, &sqNrCheck);
			if(sqNrGenerating && !miliTimeGenerating) {
				if(miliTime == miliTimeCheck && sqNr <= sqNrCheck) {
					sqNr = sqNrCheck+1;
				}
			}
			if(miliTime < miliTimeCheck || (!sqNrGenerating && miliTime == miliTimeCheck && sqNr <= sqNrCheck)) {
				strcpy(responseBuffer, "-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
				return;
			}
		}

		if (streamIndex == -1) {
				streamIndex = streamCount++;
				strncpy(streams[streamIndex].key, args[1], sizeof(streams[streamIndex].key) - 1);
				streams[streamIndex].entriesCount = 0;
		}

		struct entries *entry = &streams[streamIndex].entries[streams[streamIndex].entriesCount];

		entry->pairsCount = 0;

		for (int j = 3; j+1 < argsCount; j += 2) {
				int pIdx = entry->pairsCount;
				if (pIdx < BUFFER_SIZE) {
						strncpy(entry->pairs[pIdx].key, args[j], BUFFER_SIZE - 1);
						strncpy(entry->pairs[pIdx].value, args[j+1], BUFFER_SIZE - 1);
						entry->pairsCount++;
				}
		}
		char id_string[64];
		if(miliTime == 0 && sqNr == 0) {
			sqNr = 1;
		}
		printf("miliTime: %llu, sqNr: %llu\n", miliTime, sqNr);
		if(miliTimeGenerating) {
			miliTime = get_current_time_ms();
			sqNr = 0;
		}
		snprintf(id_string, sizeof(id_string), "%llu-%llu", miliTime, sqNr);
		strncpy(entry->id, id_string, sizeof(entry->id) - 1);
		streams[streamIndex].entriesCount++;
		sprintf(responseBuffer, "$%zu\r\n%s\r\n", strlen(id_string), id_string);
	}else if(strcasecmp(args[0], "XRANGE") == 0) {
		long long unsigned int startTimeMili = 0, startTimeSqNr = 0, endTimeMili = 0, endTimeSqNr = 0;
		char startTimeString[64], endTimeString[64], startTimeSqNrString[64], endTimeSqNrString[64];
		sscanf(args[2], "%[^-]-%s", startTimeString ,startTimeSqNrString);
		startTimeMili = strtoull(startTimeString, NULL, 10);
		startTimeSqNr = strtoull(startTimeSqNrString, NULL, 10);
		sscanf(args[3], "%[^-]-%s", endTimeString ,endTimeSqNrString);
		endTimeMili = strtoull(endTimeString, NULL, 10);
		endTimeSqNr = strtoull(endTimeSqNrString, NULL, 10);
		if(strcmp(args[2], "-") == 0) {
			startTimeMili = 0;
			startTimeSqNr = 0;
		}
		if(strcmp(args[3], "+") == 0) {
			endTimeMili = (1ULL << 63) - 1;
			endTimeSqNr = (1ULL << 63) - 1;
		}
		int startI = -1, endI = 0;
		int streamIndex = -1;
		for (int i = 0; i < streamCount; i++) {
			if (strcmp(args[1], streams[i].key) == 0) {
				for(int j = 0;j<streams[i].entriesCount;j++) {
					long long unsigned int entryTime, entrySqNr;
					streamIndex = i;
					sscanf(streams[i].entries[j].id, "%llu-%llu", &entryTime, &entrySqNr);
					if(entryTime >= startTimeMili && entrySqNr >= startTimeSqNr && startI == -1) startI = j;
					else if(entryTime <= endTimeMili && entrySqNr <= endTimeSqNr) {
						endI = j;
					}else break;
				}
				break;
			}
		}
		char token[2078];
		sprintf(token, "*%i\r\n", endI-startI+1);
		strcat(responseBuffer, token);
		printf("startI: %i, endI: %i\n", startI, endI);
		for(int i = startI;i<=endI;i++) {
			sprintf(token, "*2\r\n$%zu\r\n%s\r\n", strlen(streams[streamIndex].entries[i].id), streams[streamIndex].entries[i].id);
			strcat(responseBuffer, token);
			sprintf(token, "*%i\r\n", streams[streamIndex].entries[i].pairsCount*2);
			strcat(responseBuffer, token);
			for(int j = 0;j<streams[streamIndex].entries[i].pairsCount;j++) {
				sprintf(token, "$%zu\r\n%s\r\n", strlen(streams[streamIndex].entries[i].pairs[j].key), streams[streamIndex].entries[i].pairs[j].key);
				strcat(responseBuffer, token);
				sprintf(token, "$%zu\r\n%s\r\n", strlen(streams[streamIndex].entries[i].pairs[j].value), streams[streamIndex].entries[i].pairs[j].value);
				strcat(responseBuffer, token);
			}
		}
	}else if(strcasecmp(args[0], "XREAD") == 0) {
		int argsK = 0;
		if(strcasecmp(args[1], "BLOCK") == 0) {
			argsK = 0;
			blocksStream[blocksStreamCount].miliseconds = atoi(args[2]) + get_current_time_ms();
			blocksStream[blocksStreamCount].clientFd = clientFd;
			int t = (argsCount-4)/2;
			for(int i = 0;i<t;i++) {
				strcpy(blocksStream[blocksStreamCount].pairs[i].key, args[i+4]);
				strcpy(blocksStream[blocksStreamCount].pairs[i].value, args[i+4+t]);
				blocksStream[blocksStreamCount].pairsCount++;
			}
		}
		if(strcasecmp(args[1], "STREAMS") == 0) {
			int k = (argsCount-2)/2;
			char token[2074];
			
			sprintf(token, "*%i\r\n", k);
			strcat(responseBuffer, token);
			for(int i = 0;i<k;i++) {
				strcat(responseBuffer, "*2\r\n");
				sprintf(token, "$%zu\r\n%s\r\n", strlen(args[i+2+argsK]), args[i+2 + argsK]);
				strcat(responseBuffer, token);
				char end[4096];
				end[0] = '\0';
				int matchCount = 0;
				long long unsigned int mili = 0, sq = 0;
				sscanf(args[i+k+argsK+2], "%llu-%llu", &mili, &sq);
				for(int s = 0;s<streamCount;s++) {
					if(strcmp(streams[s].key, args[i+2+argsK]) == 0) {
						for(int j = 0;j<streams[s].entriesCount;j++) {
							long long unsigned int entryMili, entrySq;
							sscanf(streams[s].entries[j].id, "%llu-%llu", &entryMili, &entrySq);
							if(entryMili > mili || (entryMili == mili && entrySq > sq)) {
								matchCount++;
								strcat(end, "*2\r\n");
								struct entries entry = streams[s].entries[j];
								sprintf(token, "$%zu\r\n%s\r\n", strlen(entry.id), entry.id);
								strcat(end, token);
								sprintf(token, "*%i\r\n", entry.pairsCount*2);
								strcat(end, token);
								for(int p = 0;p<entry.pairsCount;p++) {
									sprintf(token, "$%zu\r\n%s\r\n", strlen(entry.pairs[p].key), entry.pairs[p].key);
									strcat(end, token);
									sprintf(token, "$%zu\r\n%s\r\n", strlen(entry.pairs[p].value), entry.pairs[p].value);
									strcat(end, token);
								}
							}
						}
					}
				}
				sprintf(token, "*%i\r\n", matchCount);
				strcat(responseBuffer, token);
				strcat(responseBuffer, end);
			}
		}
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