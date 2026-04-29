#ifndef COMMANDS_H
#define COMMANDS_H

void execute(char *args[], int argsCount, char *responseBuffer, int clientFd, struct clientSession *clientSession);
int parseRespRequest(char *buffer, char *args[]);
void notifiyKeyChange(char *key, struct clientSession *currentClient);
void unwatch(struct clientSession *clientSession);
void discard(struct clientSession *clientSession);
int unwatchFunc(char *responseBuffer, struct clientSession *clientSession);
int watch(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession);
int discardFunc(char *responseBuffer, struct clientSession *clientSession);
int exec(char *responseBuffer, struct clientSession *clientSession);
int multiQueue(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession, int clientFd);
void echo(char *args[], int argsCount, char *responseBuffer);
void set(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession);
void get(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession);
void type(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession);
void incr(char *args[], int argsCount, char *responseBuffer, struct clientSession *clientSession);
void multi(char *responseBuffer, struct clientSession *clientSession, int clientFd);
void info(char *serverRole, char *masterReplicationId, char *masterReplicationOffset, char *responseBuffer);
void replconf(char *responseBuffer);
void psync(char *responseBuffer, char *masterReplicationId, int clientFd, int *replicaFds, int *replicaCount);
void pong(char *responseBuffer);
void propagate(char *args[], int argsCount, int *replicaFds, int replicaCount);

#endif