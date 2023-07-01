CC = gcc

all: server subscriber

server: server.c
	$(CC) -o server server.c

subscriber: subscriber.c
	$(CC) -o subscriber subscriber.c

clean:
	rm -f server subscriber
