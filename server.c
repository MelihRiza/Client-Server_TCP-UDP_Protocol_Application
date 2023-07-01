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
#define STDIN_FILENO 0

struct udp_message {
    char topic[50];
    u_int8_t type;
    char message[1500];
};

// ------------------ STRUCTURE TO HANDLE ONLINE CLIENTS ----------------------- //

// NODE REPRESENTING AN ONLINE CLIENT
typedef struct Node_Online_Client {
    char* data;
    int fd;
    struct Node_Online_Client* next;
} Node_Online_Client;

//  LIST FOR STORING THE ONLINE CLIENTS AT ANY TIME
typedef struct List_Online_Clients {
    Node_Online_Client* head;
} List_Online_Clients;


Node_Online_Client* create_node_online_client(char* data, int fd) {
    Node_Online_Client* newNode = (Node_Online_Client*)malloc(sizeof(Node_Online_Client));
    newNode->data = (char*)malloc(strlen(data) + 1);  
    strcpy(newNode->data, data);                    
    newNode->data[strlen(data)] = '\0';
    newNode->fd = fd;            
    newNode->next = NULL;
    return newNode;
}

List_Online_Clients* create_list_online_clients() {
    List_Online_Clients* newList = (List_Online_Clients*)malloc(sizeof(List_Online_Clients));
    newList->head = NULL;
    return newList;
}

void add_connected_client(List_Online_Clients* list, char* data, int fd) {
    Node_Online_Client* newNode = create_node_online_client(data, fd);
    if (list->head == NULL) {
        list->head = newNode;
    } else {
        Node_Online_Client* currentNode = list->head;
        while (currentNode->next != NULL) {
            currentNode = currentNode->next;
        }
        currentNode->next = newNode;
    }
}

void remove_connected_client(List_Online_Clients* list, char* data) {
    Node_Online_Client* currentNode = list->head;
    Node_Online_Client* previousNode = NULL;
    while (currentNode != NULL) {
        if (strcmp(currentNode->data, data) == 0) {
            if (previousNode == NULL) {
                list->head = currentNode->next;
            } else {
                previousNode->next = currentNode->next;
            }
            free(currentNode->data);
            free(currentNode);
            return;
        }
        previousNode = currentNode;
        currentNode = currentNode->next;
    }
}


int list_contains_connected_client(List_Online_Clients* list, char* data) {
    Node_Online_Client* currentNode = list->head;
    while (currentNode != NULL) {
        if (strcmp(currentNode->data, data) == 0) {
            return 1;
        }
        currentNode = currentNode->next;
    }
    return 0;
}

Node_Online_Client* find_client(List_Online_Clients* connected_clients, char* id) {
    Node_Online_Client* current_client = connected_clients->head;
    while (current_client != NULL) {
        if (strcmp(current_client->data, id) == 0) {
            return current_client;
        }
        current_client = current_client->next;
    }
    return NULL;
}

// FOR ME USED FOR DEBUGGING
void print_list_of_connected_clients(List_Online_Clients* list) {
    Node_Online_Client* currentNode = list->head;
    while (currentNode != NULL) {
        printf("%s ", currentNode->data);
        currentNode = currentNode->next;
    }
}

// ------------------ STRUCTURE TO HANDLE OFFLINE CLIENTS ----------------------- //

// NODE CONTAINING A MESSAGE (TO BE SENT WHEN USER COMES ONLINE)
typedef struct Node_Message {
    char* message;
    struct Node_Message* next;
} Node_Message;

// LIST CONTAINING THE MESSAGES NODES
typedef struct List_Messages {
    Node_Message* head;
} List_Messages;

// NODE CONTAINING OFFLINE CLIENT'S INFO
typedef struct Node_Offline_Client {
    char* id;
    List_Messages* messages_of_topic;
    struct Node_Offline_Client* next;
} Node_Offline_Client;

// LIST FOR OFFLINE CLIENTS
typedef struct List_Offline_Clients {
    Node_Offline_Client* head;
} List_Offline_Clients;

List_Offline_Clients* create_list_offline_clients() {
    List_Offline_Clients* new_list_offline_clients = (List_Offline_Clients*) malloc(sizeof(List_Offline_Clients));
    new_list_offline_clients->head = NULL;
    return new_list_offline_clients;
}

List_Messages* create_list_messages() {
    List_Messages* new_list_messages = (List_Messages*) malloc(sizeof(List_Messages));
    new_list_messages->head = NULL;
    return new_list_messages;
}

Node_Offline_Client* create_offline_client_node(char* id) {
    Node_Offline_Client* new_offline_client_node = (Node_Offline_Client*) malloc(sizeof(Node_Offline_Client));
    new_offline_client_node->id = (char*) malloc(strlen(id) + 1);
    strcpy(new_offline_client_node->id, id);
    new_offline_client_node->id[strlen(id)] = '\0';
    new_offline_client_node->next = NULL;

    new_offline_client_node->messages_of_topic = create_list_messages();
    return new_offline_client_node;
}

Node_Message* create_message_node(char* message) {
    Node_Message* new_message_node = (Node_Message*) malloc(sizeof(Node_Message));
    new_message_node->message = (char*) malloc(strlen(message) + 1);
    strcpy(new_message_node->message, message);
    new_message_node->message[strlen(message)] = '\0';
    
    new_message_node->next = NULL;
    return new_message_node;
}

void add_offline_client(List_Offline_Clients* list_offline_clients, char* id) {
    Node_Offline_Client* new_offline_client = create_offline_client_node(id);
    if (list_offline_clients->head == NULL) {
        list_offline_clients->head = new_offline_client;
    } else {
        Node_Offline_Client* current_offline_client = list_offline_clients->head;
        while (current_offline_client->next != NULL) {
            current_offline_client = current_offline_client->next;
        }
        current_offline_client->next = new_offline_client;
    }
}

int contains_offline_client(List_Offline_Clients* list_offline_clients, char* id) {
    Node_Offline_Client* current_offlie_client = list_offline_clients->head;
    while (current_offlie_client != NULL) {
        if (strcmp(current_offlie_client->id, id) == 0) {
            return 1;
        }
        current_offlie_client = current_offlie_client->next;
    }
    return 0;
}


void add_messge_to_offline_client(List_Offline_Clients* list_offline_clients, char* id, char* message) {
    Node_Message* new_message = create_message_node(message);
    if (contains_offline_client(list_offline_clients, id)) {
        Node_Offline_Client* current_offline_client = list_offline_clients->head;
        while (current_offline_client != NULL) {
            if (strcmp(current_offline_client->id, id) == 0) {
                if (current_offline_client->messages_of_topic->head == NULL) {
                    current_offline_client->messages_of_topic->head = new_message;
                } else {
                    Node_Message* current_message_node = current_offline_client->messages_of_topic->head;
                    while (current_message_node->next != NULL) {
                        current_message_node = current_message_node->next;
                    }
                    current_message_node->next = new_message;
                }
                break;
            }
            current_offline_client = current_offline_client->next;
        }
    } else {
        add_offline_client(list_offline_clients, id);
        Node_Offline_Client* current_offline_client = list_offline_clients->head;
        while (current_offline_client->next != NULL) {
            current_offline_client = current_offline_client->next;
        }
        current_offline_client->messages_of_topic->head = new_message;
    }
}

// FOR ME USED FOR DEBUGGING
void print_offline_clients_and_messages(List_Offline_Clients* list_offline_clients) {
    Node_Offline_Client* current_offline_client = list_offline_clients->head;
    while (current_offline_client != NULL) {
        printf("id: %s ", current_offline_client->id);
        Node_Message* current_message = current_offline_client->messages_of_topic->head;
        while (current_message != NULL) {
            printf("%s ", current_message->message);
            current_message = current_message->next;
        }
        current_offline_client = current_offline_client->next;
        printf("\n");
    }
}



// ------------------ STRUCTURE TO HANDLE SUBSCRIBERS SUBSCRIBED TO TOPICS ----------------------- //

// NODE CONTAINING A SUBSCRIBER'S INFO
typedef struct Node_Subscriber {
    char* id;
    int mode;
    struct Node_Subscriber* next;
} Node_Subscriber;

typedef struct List_Subscribers {
    Node_Subscriber* head;
} List_Subscribers;

// NODE CONTAINING A TOPIC'S INFO
// ALSO A TOPIC NODE CONTAINS A LIST OF SUBSCRIBERS (SUBSCRIBED TO THIS TOPIC)
typedef struct Node_Topic {
    char* topic;
    List_Subscribers* subscribers_of_topic;
    struct Node_Topic* next;
} Node_Topic;

typedef struct List_Topics {
    Node_Topic* head;
} List_Topics;

List_Topics* create_list_topics() {
    List_Topics* new_list_topics = (List_Topics*) malloc(sizeof(List_Topics));
    new_list_topics->head = NULL;
    return new_list_topics;
}

List_Subscribers* create_list_subscribers() {
    List_Subscribers* new_list_subscribers = (List_Subscribers*) malloc(sizeof(List_Subscribers));
    new_list_subscribers->head = NULL;
    return new_list_subscribers;
}

Node_Topic* create_topic_node(char* topic) {
    Node_Topic* new_node_topic = (Node_Topic*) malloc(sizeof(Node_Topic));
    new_node_topic->topic = (char*) malloc(strlen(topic) + 1);
    strcpy(new_node_topic->topic, topic);
    new_node_topic->topic[strlen(topic)] = '\0';
    new_node_topic->next = NULL;

    new_node_topic->subscribers_of_topic = create_list_subscribers();

    return new_node_topic;
}

Node_Subscriber* create_subscriber_node(char* id, int mode) {
    Node_Subscriber* new_node_subscriber = (Node_Subscriber*) malloc(sizeof(Node_Subscriber));
    new_node_subscriber->id = (char*) malloc(strlen(id) + 1);
    strcpy(new_node_subscriber->id, id);
    new_node_subscriber->id[strlen(id)] = '\0';
    
    new_node_subscriber->mode = mode;
    new_node_subscriber->next = NULL;
    return new_node_subscriber;
}

void add_topic(List_Topics* list_topics, char* topic) {
    Node_Topic* new_node_topic = create_topic_node(topic);
    if (list_topics->head == NULL) {
        list_topics->head = new_node_topic;
    } else {
        Node_Topic* current_topic_node = list_topics->head;
        while (current_topic_node->next != NULL) {
            current_topic_node = current_topic_node->next;
        }
        current_topic_node->next = new_node_topic;
    }
}

int list_contains_topic(List_Topics* list_topics, char* topic) {
    Node_Topic* current_topic_node = list_topics->head;
    while (current_topic_node != NULL) {
        if (strcmp(current_topic_node->topic, topic) == 0) {
            return 1;
        }
        current_topic_node = current_topic_node->next;
    }
    return 0;
}

void add_subscriber_to_topic(List_Topics* list_topics, char* topic, char* id, int mode) {
    Node_Subscriber* new_subscriber_node = create_subscriber_node(id, mode);
    if (list_contains_topic(list_topics, topic)) {
        Node_Topic* current_topic_node = list_topics->head;
        while (current_topic_node != NULL) {
            if (strcmp(current_topic_node->topic, topic) == 0) {
                if (current_topic_node->subscribers_of_topic->head == NULL) {
                    current_topic_node->subscribers_of_topic->head = new_subscriber_node;
                } else {
                    Node_Subscriber* current_subscriber_node = current_topic_node->subscribers_of_topic->head;
                    while (current_subscriber_node->next != NULL) {
                        current_subscriber_node = current_subscriber_node->next;
                    }
                    current_subscriber_node->next = new_subscriber_node;
                }
                break;
            }
            current_topic_node = current_topic_node->next;
        }
    } else {
        add_topic(list_topics, topic);
        Node_Topic* current_topic_node = list_topics->head;
        while (current_topic_node->next != NULL) {
            current_topic_node = current_topic_node->next;
        }
        current_topic_node->subscribers_of_topic->head = new_subscriber_node;
    }
}

void remove_subscriber_from_topic(List_Topics* list_topics, char* topic, char* id) {
    if (list_contains_topic(list_topics, topic)) {
        Node_Topic* current_topic_node = list_topics->head;
        while (current_topic_node != NULL) {
            if (strcmp(current_topic_node->topic, topic) == 0) {
                if (strcmp(current_topic_node->subscribers_of_topic->head->id, id) == 0 
                        && current_topic_node->subscribers_of_topic->head->next == NULL) {

                    free(current_topic_node->subscribers_of_topic->head->id);
                    current_topic_node->subscribers_of_topic->head = NULL;
                    return;
                } 
                else if (strcmp(current_topic_node->subscribers_of_topic->head->id, id) == 0 
                        && current_topic_node->subscribers_of_topic->head->next != NULL) {
                    Node_Subscriber* to_be_freed = current_topic_node->subscribers_of_topic->head;
                    current_topic_node->subscribers_of_topic->head = current_topic_node->subscribers_of_topic->head->next;
                    free(to_be_freed);
                }
                else {
                    Node_Subscriber* prev_subscriber_node = current_topic_node->subscribers_of_topic->head;
                    Node_Subscriber* current_subscriber_node = current_topic_node->subscribers_of_topic->head->next;
                    while (current_subscriber_node != NULL) {
                        if (strcmp(current_subscriber_node->id, id) == 0) {
                            prev_subscriber_node->next = current_subscriber_node->next;
                            free(current_subscriber_node);
                            return;
                        }
                        prev_subscriber_node = current_subscriber_node;
                        current_subscriber_node = current_subscriber_node->next;
                    }
                }
            }
            current_topic_node = current_topic_node->next;
        }
    } else {
        return;
    }
}

// FOR ME USED FOR DEBUGING
void print_topics_and_subscribers(List_Topics* list_topics) {
    Node_Topic* current_topic_node = list_topics->head;
    while (current_topic_node != NULL) {
        printf("Topic: %s ", current_topic_node->topic);
        Node_Subscriber* current_subscriber_node = current_topic_node->subscribers_of_topic->head;
        while (current_subscriber_node != NULL) {
            printf("%s ", current_subscriber_node->id);
            current_subscriber_node = current_subscriber_node->next;
        }
        current_topic_node = current_topic_node->next;
        printf("\n");
    }
}

Node_Topic* find_topic_node(List_Topics* list_topics, char* topic) {
    Node_Topic* current_topic_node = list_topics->head;
    while (current_topic_node != NULL) {
        if (strcmp(current_topic_node->topic, topic) == 0) {
            return current_topic_node;
        }
        current_topic_node = current_topic_node->next;
    }
    return NULL;
}

// FUNCTION USED TO SEND THE message_for_topic TO ALL TOPIC'S SUBSCRIBERS
// IF THERE ARE SUBSCRIBERS WITH SF 1 WHICH ARE NOT ONLINE THE MESSAGE WILL BE STORED 
void send_topic_to_online_subscribers(List_Topics* list_topics, List_Offline_Clients* list_offline_clients,
             List_Online_Clients* connected_clients, char* topic, char* message_for_topic) {

    Node_Topic* topic_node = find_topic_node(list_topics, topic);
    if (topic_node == NULL) {
        return;
    }
    Node_Subscriber* subscriber_node = topic_node->subscribers_of_topic->head;
    while (subscriber_node != NULL) {
        if (list_contains_connected_client(connected_clients, subscriber_node->id)) {
            Node_Online_Client* client = find_client(connected_clients, subscriber_node->id);
            send(client->fd, message_for_topic, 2000, 0);
        }
        // CASE THE SUBSCRIBER IS NOT ONLINE (STORE THE MESSAGE TO BE SENT WHEN SUBSCRIBER COMES ONLINE)
        else if (subscriber_node->mode == 1 && !list_contains_connected_client(connected_clients, subscriber_node->id)) {
            add_messge_to_offline_client(list_offline_clients, subscriber_node->id, message_for_topic);
        }
        subscriber_node = subscriber_node->next;
    }
}

// SEND THE STORED MESSAGES WHEN THE CLIENT SUBSCRIBED TO A TOPIC COMES ONLINE
void send_remained_messages(List_Offline_Clients* list_offline_clients, char* id, int fd) {
    if (contains_offline_client(list_offline_clients, id)) {
        if (list_offline_clients->head->next == NULL) {
            Node_Message* current_message = list_offline_clients->head->messages_of_topic->head;
            while (current_message != NULL) {
                send(fd, current_message->message, 2000, 0);
                current_message = current_message->next;
            }
            free(list_offline_clients->head);
            list_offline_clients->head = NULL;
        }
        else {
            Node_Offline_Client* head_offline_client = list_offline_clients->head;
            if (strcmp(head_offline_client->id, id) == 0) {
                Node_Message* current_message = head_offline_client->messages_of_topic->head;
                while (current_message != NULL) {
                    send(fd, current_message->message, 2000, 0);
                    current_message = current_message->next;
                }
                list_offline_clients->head = head_offline_client->next;
                free(head_offline_client);
                return;
            }
            Node_Offline_Client* prev_offline_client = list_offline_clients->head;
            Node_Offline_Client* current_offline_client = list_offline_clients->head->next;
            while (current_offline_client != NULL) {
                if (strcmp(current_offline_client->id, id) == 0) {
                    Node_Message* current_message = current_offline_client->messages_of_topic->head;
                    while (current_message != NULL) {
                        send(fd, current_message->message, 2000, 0);
                        current_message = current_message->next;
                    }
                    prev_offline_client->next = current_offline_client->next;
                    free(current_offline_client);
                    return;
                }
                prev_offline_client = current_offline_client;
                current_offline_client = current_offline_client->next;
            }
        }
    } else {
        return;
    }
}

// CONSTRUCT THE MESSAGE RECEIVED FROM THE UDP CLIENTS 
// SO IT WILL HAVE THE READABLE FORM, TO BE SENT TO TCP USERS
char* construct_message_from_topic(struct udp_message* message_udp) {
    char* message = (char*) calloc(sizeof(char), 2000);

    if (message_udp->type == 0) {
        strcpy(message, message_udp->topic);
        strcat(message, " - INT - ");
        char sb = message_udp->message[0];

        uint32_t value = (((unsigned char)message_udp->message[4]) << 24) |
                    (((unsigned char)message_udp->message[3]) << 16) |
                    (((unsigned char)message_udp->message[2]) << 8) |
                    (((unsigned char)message_udp->message[1]));

        char* integer_as_string = (char*) calloc(sizeof(char), 11);
   
        sprintf(integer_as_string, "%d", ntohl(value));

        if (sb == 0) {
            strcat(message, integer_as_string);
        } else if (sb == 1){
            strcat(message, "-");
            strcat(message, integer_as_string);
        }
        
    } 
    else if (message_udp->type == 1) {
       strcpy(message, message_udp->topic);
       strcat(message, " - SHORT_REAL - ");

        u_int16_t value = (((unsigned char)message_udp->message[0]) << 8) |
                    ((unsigned char)message_udp->message[1]);

        
        char* short_real_as_string = (char*) calloc(sizeof(char), 11);
        
        sprintf(short_real_as_string, "%d", value);

        int len = strlen(short_real_as_string);
        if (len >= 2) {
            char* temp = (char*) calloc(sizeof(char), 3);
            strcpy(temp, &short_real_as_string[len - 2]);
            short_real_as_string[len - 2] = '\0';
            strcat(short_real_as_string, ".");
            strcat(short_real_as_string, temp);
            free(temp);
        }
        
        strcat(message, short_real_as_string);
    }
    else if (message_udp->type == 2) {
        strcpy(message, message_udp->topic);
        strcat(message, " - FLOAT - ");
        char sb = message_udp->message[0];

        uint32_t value = ntohl((((unsigned char)message_udp->message[4]) << 24) |
                    (((unsigned char)message_udp->message[3]) << 16) |
                    (((unsigned char)message_udp->message[2]) << 8) |
                    (((unsigned char)message_udp->message[1])));

        char negative_power = message_udp->message[5];

        char* int_as_string = (char*) calloc(sizeof(char), 11);

        sprintf(int_as_string, "%d", value);

        int len = 1 + strlen(int_as_string);
        if (negative_power >= len - 1) {
            len += negative_power - strlen(int_as_string) + 1;
        }  

        char* float_as_string = (char*) calloc(sizeof(char), 11);

        int i = 0;
        int nr_digits = strlen(int_as_string);
        if (negative_power >= nr_digits) {
            float_as_string[i++] = '0';
            float_as_string[i++] = '.';
            for (int j = 0; j < negative_power - nr_digits; j++) {
                float_as_string[i++] = '0';
            }
        }
        for (int j = 0; j < nr_digits; j++) {
            if (j == nr_digits - negative_power) {
                float_as_string[i++] = '.';
            }
            float_as_string[i++] = int_as_string[j];
        }
        float_as_string[i] = '\0';

        if (sb == 0) {
            strcat(message, float_as_string);
        }
        else if (sb == 1) {
            strcat(message, "-");
            strcat(message, float_as_string);
        }
    }   
    else if (message_udp->type == 3) {
        strcpy(message, message_udp->topic);
        strcat(message, " - STRING - ");
       
        strcat(message, message_udp->message);
    }

    return message;
}

char* from_int_to_ipv4_format(int ip) {
    char* result = (char*) malloc(sizeof(char) * 16);
    sprintf(result, "%u.%u.%u.%u", (ip) & 0xff, (ip >> 8) & 0xff, (ip >> 16) & 0xff, (ip >> 24) & 0xff);
    return result;
}



int main(char argc, char *argv[]) {
    int buffering_set = setvbuf(stdout, NULL, _IONBF, BUFSIZ);
    DIE(buffering_set < 0, "Error setting off buffering!\n");

    struct pollfd *pfds = malloc(sizeof(struct pollfd) * MAX_PFDS);

    int nr_fds = 0;

    pfds[nr_fds].fd = STDIN_FILENO;
    pfds[nr_fds].events = POLLIN;
    nr_fds++;

    int socket_listen_tcp, socket_client;
    struct sockaddr_in server_addr, server_addr_udp, client_addr;
    char server_message[2000], client_message[2000];

    memset(server_message, 0, sizeof(server_message));
    memset(client_message, 0, sizeof(client_message));
    memset(&server_addr, 0, sizeof(server_addr));
    memset(&client_addr, 0, sizeof(client_addr));

    // CREATE SOCKET FOR LISTENING
    socket_listen_tcp = socket(AF_INET, SOCK_STREAM, 0);
    DIE(socket_listen_tcp < 0, "Error while creating socket for listening!\n");
    int enable = 1;
    setsockopt(socket_listen_tcp, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int));
    setsockopt(socket_listen_tcp, SOL_SOCKET, TCP_NODELAY, &enable, sizeof(int));

    // SETTING UP SERVER
    int port = atoi(argv[1]);
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(port);
    server_addr.sin_addr.s_addr = inet_addr("127.0.0.1");

    // BINDING
    int binded = bind(socket_listen_tcp, (struct sockaddr*)&server_addr, sizeof(server_addr));
    DIE(binded < 0, "Error while binding!\n");

    // LISTENING
    int listened = listen(socket_listen_tcp, 1);
    DIE(listened < 0, "Error while listening!\n");

    pfds[nr_fds].fd = socket_listen_tcp;
    pfds[nr_fds].events = POLLIN;
    nr_fds++;


    // UDP SOCKET SETTING UP
        struct pollfd *pfds_udp = malloc(sizeof(struct pollfd) * 20);
        int nr_fds_udp = 0;

        int sock_udp = socket(AF_INET, SOCK_DGRAM, 0);
        DIE(sock_udp < 0, "Error while setting an UDP socket!\n");

        pfds_udp[nr_fds_udp].fd = sock_udp;
        pfds_udp[nr_fds_udp].events = POLLIN;
        nr_fds_udp++;

        struct sockaddr_in addr_udp;
        addr_udp.sin_family = AF_INET;
        addr_udp.sin_port = htons(port); 
        addr_udp.sin_addr.s_addr = INADDR_ANY;

        int bind_udp = bind(sock_udp, (struct sockaddr*)&addr_udp, sizeof(addr_udp));
        DIE(bind_udp < 0, "Error while binding udp socket!\n");

    List_Online_Clients *connected_clients = create_list_online_clients();
    List_Topics *list_topics = create_list_topics();
    List_Offline_Clients* list_offline_clients = create_list_offline_clients();

    while(1) {
        poll(pfds, nr_fds, 2);
        int result_listen_udp = poll(pfds_udp, nr_fds_udp, 10);

        if (result_listen_udp >= 0) {
            for (int i = 0; i < nr_fds_udp; i++) {
                if (pfds_udp[i].revents & POLLIN) {
                    struct udp_message *message_udp = calloc(sizeof(struct udp_message), 1);
                    
                    struct sockaddr_in addr;
                    int addr_len = sizeof(addr);
                    int len = recvfrom(pfds_udp[i].fd, message_udp, sizeof(struct udp_message),
                            0, (struct sockaddr*)&addr, &addr_len);

                    char* message_for_topic = (char*) calloc(sizeof(char), 1600);

                    message_for_topic = construct_message_from_topic(message_udp);  

                    char* message_for_client = (char*) calloc(sizeof(char), 2000);

                    char* ip_udp = (char*) calloc(sizeof(char), 16);
                    char* port_udp = (char*) calloc(sizeof(char), 8);

                    ip_udp = from_int_to_ipv4_format(addr.sin_addr.s_addr);
                    sprintf(port_udp, "%d", addr.sin_port);

                    strcat(message_for_client, ip_udp);
                    strcat(message_for_client, ":");
                    strcat(message_for_client, port_udp);
                    strcat(message_for_client, " - ");
                    strcat(message_for_client, message_for_topic); 
          
                    send_topic_to_online_subscribers(list_topics, list_offline_clients, connected_clients, message_udp->topic, message_for_client);
                    free(message_for_topic);
                }
            }
        }
    
        // NEW CONNECTION WITH A CLIENT NEEDS TO BE ESTABLISHED
        if ((pfds[1].revents & POLLIN) != 0) {

            int client_size = sizeof(client_addr);
            socket_client = accept(socket_listen_tcp, (struct sockaddr*)&client_addr, &client_size);
            setsockopt(socket_client, SOL_SOCKET, TCP_NODELAY, &enable, sizeof(int));
            
            char *client_id = calloc(sizeof(char), 10);
            int rcv = recv(socket_client, client_id, 10, 0);
            DIE(rcv < 0, "Error while receiving client's ID!\n");
            setsockopt(socket_client, SOL_SOCKET, TCP_NODELAY, &enable, sizeof(int));
            
            // CLIENT CONNECT
            if (!list_contains_connected_client(connected_clients, client_id)) {
                pfds[nr_fds].fd = socket_client;
                pfds[nr_fds].events = POLLIN;
                nr_fds++;
                setsockopt(pfds[nr_fds].fd, SOL_SOCKET, TCP_NODELAY, &enable, sizeof(int));

                send_remained_messages(list_offline_clients, client_id, socket_client);

                add_connected_client(connected_clients, client_id, socket_client);

                char* ip = (char*) malloc(sizeof(char) * 16);
                ip = from_int_to_ipv4_format(client_addr.sin_addr.s_addr);

                printf("New client %s connected from %s:%d.\n", client_id, ip, client_addr.sin_port);
            } 
            // CLIENT ALREADY CONNECTED
            else {
                send(socket_client, "already connected", 20, 0);
                close(socket_client);
                printf("Client %s already connected.\n", client_id);
            }
    
            free(client_id);
            
        }
        // STDIN FOR SERVER: COMMAND CAN ONLY BE "exit" FOR THE SERVER TO CLOSE
        else if ((pfds[0].revents & POLLIN) != 0) {
            char *server_command = malloc(sizeof(char) * 10);
            int rcv = read(STDIN_FILENO, server_command, 10);
            if (strncmp(server_command, "exit", 4) == 0) {
                for (int i = 2; i <= nr_fds; i++) {
                    send(pfds[i].fd, "server is closing", 21, 0);
                }
                close(socket_listen_tcp);
                close(socket_client);
                break;
            }
        } 
        // CLIENT SENT COMMAND TO SERVER
        else {
            for (int i = 2; i <= nr_fds; i++) {
                if ((pfds[i].revents & POLLIN) != 0) {

                    char *client_message = malloc(100 * sizeof(char));
                    recv(pfds[i].fd, client_message, 100, 0);

                    // CLIENT EXITING SO CLOSE CONNECTION WITH HIM AFTER SENDING "ack exiting"
                    if (strncmp(client_message, "exiting", 7) == 0) {
                        send(pfds[i].fd, "ack exiting", 12, 0);

                        char* command = malloc(sizeof(char) * 10);
                        char* client = malloc(sizeof(char) * 10);

                        sscanf(client_message, "%s %s", command, client);
                    
                        printf("Client %s disconnected.\n", client);
                        close(pfds[i].fd);

                        remove_connected_client(connected_clients, client);

                        int j;
                        for (j = i; j < nr_fds; j++) {
                            pfds[j] = pfds[j+1];
                        }
                        nr_fds--;

                        free(command);
                        free(client);
                    }
                    else {

                        char* id = malloc(sizeof(char) * 10);
                        char* command = malloc(sizeof(char) * 11);
                        char* topic = malloc(sizeof(char) * 51);
                        int mode;

                        sscanf(client_message, "%s %s %s %d", id, command, topic, &mode);

                        if (strcmp(command, "subscribe") == 0) {
                            add_subscriber_to_topic(list_topics, topic, id, mode);

                            send(pfds[i].fd, "ack subscription", 18, 0);
                           
                        } 
                        else if (strcmp(command, "unsubscribe") == 0) {
                            remove_subscriber_from_topic(list_topics, topic, id);

                            send(pfds[i].fd, "ack unsubscribing", 19, 0);
                        }

                        free(id);
                        free(command);
                        free(topic);
                    }
                    
                    free(client_message);

                }
            }
        }
    }

    return 0;
}