#include <iostream>
#include <cstdlib>
#include <string>
#include <cstring>
#include <unistd.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/event.h>
#include <sys/time.h>

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
    // kq is what we use for all sub operations to tell the kernelt what we want to monitor and to retrieve which events that occured
    // Like a mailbox with subs (file descriptors to watch) and pick up mail (events that happened)

    int kq = kqueue();
    if (kq == -1) {
        std::cerr << "Creation of kqueue failed\n";
        return 1;
    }

    struct kevent change; // the thing we want to watch/monitor
    EV_SET(&change, server_fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr); // can view as init
    kevent(kq, &change, 1, nullptr, 0, nullptr);

    struct kevent event; // receiving the event

    while (true) {
        int num_events = kevent(kq, nullptr, 0, &event, 1, nullptr); // also like init
        if (num_events < 0)
        {
            std::cerr << "epoll_wait failed\n";
            break; // You might want more graceful handling
        }

        if (event.ident == server_fd && event.filter == EVFILT_READ) {
            struct sockaddr_in client_addr;
            int client_addr_len = sizeof(client_addr);
            std::cout << "Waiting for a client to connect...\n";

            // You can use print statements as follows for debugging, they'll be visible when running tests.
            std::cout << "Logs from your program will appear here!\n";

            int client_fd = accept(server_fd, (struct sockaddr *)&client_addr, (socklen_t *)&client_addr_len);

            std::cout << "Client connected\n";

            // Subscribe client socket 
            EV_SET(&change, client_fd, EVFILT_READ, EV_ADD | EV_ENABLE, 0, 0, nullptr); // can view as init
            kevent(kq, &change, 1, nullptr, 0, nullptr);
        } else {
            // This should be client socket 
            int fd = static_cast<int>(event.ident);
            
            char buffer[1024];
            int bytes_recv;

            bytes_recv = recv(fd, buffer, sizeof(buffer) - 1, 0); // Receive up to 1023 bytes
            if (bytes_recv > 0)
            {
                buffer[bytes_recv] = '\0'; // Null-terminate received data
                std::cout << "Received: " << buffer << std::endl; // Print received data
                char *msg = "+PONG\r\n";
                int msg_len = strlen(msg);

                send(fd, msg, msg_len, 0);
            }
            else if (bytes_recv == 0)
            {
                std::cout << "Client disconnected." << std::endl;
                break;
            }
            else
            {
                std::cout << "Error in recv - " << std::endl;
                break;
            }
        }

    }


    close(server_fd);

    return 0;
}
