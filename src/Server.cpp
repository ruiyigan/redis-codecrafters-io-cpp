#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/epoll.h>


int main(int argc, char **argv)
{
    // Flush after every std::cout / std::cerr
    std::cout << std::unitbuf;
    std::cerr << std::unitbuf;

    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0)
    {
        std::cerr << "Failed to create server socket\n";
        return 1;
    }

    // Since the tester restarts your program quite often, setting SO_REUSEADDR
    // ensures that we don't run into 'Address already in use' errors
    int reuse = 1;
    if (setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0)
    {
        std::cerr << "setsockopt failed\n";
        return 1;
    }

    struct sockaddr_in server_addr;
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(6379);

    if (bind(server_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) != 0)
    {
        std::cerr << "Failed to bind to port 6379\n";
        return 1;
    }

    int connection_backlog = 5;
    if (listen(server_fd, connection_backlog) != 0)
    {
        std::cerr << "listen failed\n";
        return 1;
    }

    // Creates the event queue in the kernel and returns the file descriptor referencing it
    // kq/epoll_fd is what we use for all sub operations to tell the kernelt what we want to monitor and to retrieve which events that occured
    // Like a mailbox with subs (file descriptors to watch) and pick up mail (events that happened)
    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1) {
        perror("epoll_create1");
        close(server_fd);
        return 1;
    }

    struct epoll_event ev;
    ev.events = EPOLLIN; 
    ev.data.fd = server_fd;

    // Add serverSocket to the epoll instance
    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1) {
        perror("epoll_ctl: server_fd");
        close(server_fd);
        close(epoll_fd);
        return 1;
    }

    while (true) {
        struct epoll_event events[64]; 
        int num_events = epoll_wait(epoll_fd, events, 64, -1);
        if (num_events < 0)
        {
            std::cerr << "epoll_wait failed\n";
            break; // You might want more graceful handling
        }

        for (int i = 0; i < num_events; i++) {
            int fd = events[i].data.fd;
            uint32_t eventMask = events[i].events;

            if (fd == server_fd && (eventMask & EPOLLIN)) {
                struct sockaddr_in client_addr;
                int client_addr_len = sizeof(client_addr);
                std::cout << "Waiting for a client to connect...\n";
                int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);

                std::cout << "Client connected\n";

                struct epoll_event clientEv;
                clientEv.events = EPOLLIN; 
                clientEv.data.fd = client_fd;

                if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, client_fd, &clientEv) == -1) {
                    perror("epoll_ctl: clientSocket");
                    close(client_fd);
                }
            } else {
                char buffer[1024];
                int bytes_recv;

                bytes_recv = recv(fd, buffer, sizeof(buffer) - 1, 0); // Receive up to 1023 bytes
                if (bytes_recv > 0)
                {
                    buffer[bytes_recv] = '\0'; // Null-terminate received data
                    std::cout << "Received: " << buffer << std::endl; // Print received data
                    const char* msg = "+PONG\r\n";
                    int msg_len = strlen(msg);

                    send(fd, msg, msg_len, 0);
                }
                else if (bytes_recv == 0)
                {
                    std::cout << "Client disconnected." << std::endl;
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr); // If don't do this, the socket stays in the epoll set and will keep seeing the client disconnected message as kernel is notifying you that nothing left to read 
                    close(fd);
                }
                else
                {
                    std::cout << "Error in recv - " << std::endl;
                    epoll_ctl(epoll_fd, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                }
            }

        }

    }

    close(server_fd);

    return 0;
}
