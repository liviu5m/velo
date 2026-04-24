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
#include "types.h"

#define BUFFER_SIZE 2048

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


int executeLists(char *args[], int argsCount, char *responseBuffer, int clientFd) {
  if(strcasecmp(args[0], "RPUSH") == 0) {
		for(int i = 0;i<listCount;i++) {
			if(strcmp(lists[i].key, args[1]) == 0) {
				for(int j = 2;j<argsCount;j++) {
					lists[i].values[lists[i].valuesCount] = strdup(args[j]);
					lists[i].valuesCount++;
				}
				sprintf(responseBuffer, ":%i\r\n", lists[i].valuesCount);
				checkBlop();
				return 1;
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
    return 1;
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
				return 1;
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
    return 1;
	}else if(strcasecmp(args[0], "LLEN") == 0) {
		for(int i = 0;i<listCount;i++) {
			if(strcmp(lists[i].key, args[1]) == 0) {
				sprintf(responseBuffer,":%i\r\n", lists[i].valuesCount);
				return 1;
			}
		}
		sprintf(responseBuffer,":0\r\n");
    return 1;
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
				
				return 1;
			}
		}
		sprintf(responseBuffer,"$-1\r\n");
    return 1;
	}else if(strcasecmp(args[0], "BLPOP") == 0) {
		for(int j = 0;j<listCount;j++) {
			if(strcmp(lists[j].key, args[1]) == 0) {
				sprintf(responseBuffer, "*2\r\n$%zu\r\n%s\r\n$%zu\r\n%s\r\n", strlen(lists[j].key), lists[j].key, strlen(lists[j].values[0]), lists[j].values[0]);
				for(int t = 1;t<lists[j].valuesCount;t++) {
					lists[j].values[t-1] = lists[j].values[t];
				}
				lists[j].valuesCount--;
				lists[j].values[lists[j].valuesCount] = NULL;
				
				return 1;
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
		return 1;
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
				return 1;
			}
		}
		sprintf(responseBuffer, "*0\r\n");
    return 1;
	}
  return 0;
}