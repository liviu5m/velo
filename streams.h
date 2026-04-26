#ifndef STREAMS_H
#define STREAMS_H

void checkBlockStreams();
int executeStreams(char *args[], int argsCount, char *responseBuffer, int clientFd);

#endif