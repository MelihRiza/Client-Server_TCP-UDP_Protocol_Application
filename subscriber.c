#include <stdio.h>
#include <stdlib.h>
#include <poll.h>
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <unistd.h>

#include "helpers.h"


#define MAX_PFDS 32


int main(char argc, char *argv[]) {


    setvbuf(stdout, NULL, _IONBF, BUFSIZ);

    int socket_server;
    struct sockaddr_in server_addr;
    char server_message[2000], client_message[2000];

    memset(server_message,'\0',sizeof(server_message));
    memset(client_message,'\0',sizeof(client_message));
    memset(&server_addr, 0, sizeof(server_addr));

    socket_server = socket(AF_INET, SOCK_STREAM, 0);
    DIE(socket_server < 0, "Error while creating socket!\n");

    int enable = 1;
    setsockopt(socket_server, SOL_SOCKET, TCP_NODELAY, &enable, sizeof(int));
    setsockopt(socket_server, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));

    int port = atoi(argv[3]);
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr(argv[2]);

    int cnct = connect(socket_server, (struct sockaddr*)&server_addr, sizeof(server_addr));
    DIE(cnct < 0, "Error while connecting!\n");

    // SEND ITS ID TO THE SERVER
    char *id = calloc(sizeof(char), 10);
    strcpy(id, argv[1]);
    int snd = send(socket_server, id, 10, 0);
    DIE(snd < 0, "Error while sending!\n");

    // FOR LISTENING
    struct pollfd *pfds = malloc(sizeof(struct pollfd) * MAX_PFDS);

    int nr_fds = 0;

    pfds[nr_fds].fd = STDIN_FILENO;
    pfds[nr_fds].events = POLLIN;
    nr_fds++;

    pfds[nr_fds].fd = socket_server;
    pfds[nr_fds].events = POLLIN;
    nr_fds++;
    setsockopt(socket_server, SOL_SOCKET, TCP_NODELAY, &enable, sizeof(int));

    while (1) {
        poll(pfds, nr_fds, 10);

        // RECEIVING MESSAGE FROM SERVER
        if ((pfds[1].revents & POLLIN) != 0) {

            int server_size = sizeof(server_addr);
            accept(socket_server, (struct sockaddr*)&server_addr, &server_size);
            setsockopt(socket_server, SOL_SOCKET, TCP_NODELAY, &enable, sizeof(int));

            recv(pfds[1].fd, server_message, 2000, 0);
            
            // ALREADY CONNECTED OR SERVER IS CLOSING SO CLOSE CLIENT
            if ((strncmp(server_message, "already connected", 18) == 0)
                    || (strncmp(server_message, "server is closing", 20) == 0)) {
                close(pfds[1].fd);
                break;
            }
            // PRINT THE MESSAGE FROM SERVER RELATED TO A TOPIC THIS CLIENT IS SUBSCRIBED TO
            else {
                printf("%s\n", server_message);
            }

        } 
        // RECEIVING MESSAGE FROM STDIN
        else if ((pfds[0].revents & POLLIN) != 0) {

            char *self_command = malloc(sizeof(char) * 61);
            int rcv = read(STDIN_FILENO, self_command, 61);

            // GOT "exit" SO NEED TO DISCONNECT
            if (!strncmp(self_command, "exit", 4)) {
                char* exit_command = calloc(sizeof(char), 20);
                
                strcpy(exit_command, "exiting");
                strcat(exit_command, " ");
                strcat(exit_command, id);

                send(socket_server, exit_command, 20, 0);
                free(exit_command);

                recv(pfds[1].fd, server_message, 50, 0);
                
                if (strncmp(server_message, "ack exiting", 11) == 0) {
                    close(socket_server);
                    break;
                }

            } 
            // WANT TO SUBSCRIBE A TOPIC
            else if (strncmp(self_command, "subscribe", 9) == 0) {
                char* subscribe_command = malloc(sizeof(char) * 61);
                strcpy(subscribe_command, id);
                strcat(subscribe_command, " ");
                strcat(subscribe_command, self_command);      
                send(socket_server, subscribe_command, 63, 0);

                recv(pfds[1].fd, server_message, 50, 0);
                
                if (strncmp(server_message, "ack subscription", 17) == 0) {
                    printf("Subscribed to topic.\n");
                }
                else {
                    printf("Error: Did not received ack for subscription!\n");
                    break;
                }

                free(subscribe_command);
            } 
            // WANT TO UNSUBSCRIBE
            else if (strncmp(self_command, "unsubscribe", 9) == 0) {
                char* unsubscribe_command = malloc(sizeof(char) * 61);
                strcpy(unsubscribe_command, id);
                strcat(unsubscribe_command, " ");
                strcat(unsubscribe_command, self_command);      
                send(socket_server, unsubscribe_command, 61, 0);

                recv(pfds[1].fd, server_message, 50, 0);
                
                if (strncmp(server_message, "ack unsubscribing", 18) == 0) {
                    printf("Unsubscried from topic.\n");
                }
                else {
                    printf("Error: Did not received ack for unsubcribing!\n");
                    break;
                }
                free(unsubscribe_command);
            }
        }

    }

    return 0;
}