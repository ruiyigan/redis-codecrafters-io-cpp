#include <iostream>
#include <memory>       // For smart pointers and enable_shared_from_this
#include <asio.hpp>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <chrono>

using asio::ip::tcp;  // Simplify TCP namespace
using TimePoint = std::chrono::steady_clock::time_point;
using StorageType = std::unordered_map<std::string, std::tuple<std::string, std::chrono::steady_clock::time_point>>;

// Session handles each client connection. Inherits from enable_shared_from_this
// to allow safe shared_ptr management in async callbacks
class Session : public std::enable_shared_from_this<Session> {
public:
    // Constructor takes ownership of the socket
    Session(tcp::socket socket, std::shared_ptr<StorageType> storage) : socket_(std::move(socket)), storage_(storage) {}
    // Start the session's async operations
    void start() {
        read();  // Initiate first read
    }

private:
    std::vector<std::string> splitString(const std::string& input, char delimiter) {
        std::vector<std::string> tokens;
        std::stringstream stream(input);
        std::string token;

        while (std::getline(stream, token, delimiter)) {
            if (!token.empty() && token.back() == '\r') {
                token.pop_back();
            }
            tokens.push_back(token);
        }

        return tokens;
    }

    void read() {
        // Capture a shared_ptr to keep object alive during async operation
        auto self(shared_from_this());
        
        // Async read with buffer and completion handler
        socket_.async_read_some(asio::buffer(buffer_),
            // Lambda captures 'this' and self (shared_ptr) for proper lifetime
            [this, self](asio::error_code ec, std::size_t length) {
                if (!ec) {
                    // Successfully read data
                    std::string data = std::string(buffer_.data(), length);
                    std::cout << "Received: \n" << data << std::endl;
                    std::vector<std::string> split_data = splitString(data, '\n');

                    std::string message;
                    if (split_data[2] == "ECHO") {
                        // repeat
                        message = split_data.back();
                    }
                    else if (split_data[2] == "SET") {
                        // save
                        std::string key = split_data[4];
                        std::string value = split_data[6];
                        std::time_t expiry_time = 0;
                        if (split_data.size() >= 11 && split_data[8] == "px") {
                            int expiry_ms = std::stoi(split_data[10]); // milliseconds
                            auto expiry_time = std::chrono::steady_clock::now() + std::chrono::milliseconds(expiry_ms);
                            (*storage_)[key] = std::make_tuple(value, expiry_time);
                        } else {
                            // Use a distant future time if no expiry is specified
                            (*storage_)[key] = std::make_tuple(value, TimePoint::max());
                        }
                        message = "OK";
                    } 
                    else if (split_data[2] == "GET")
                    {
                        // get
                        std::string key = split_data[4];

                        auto it = storage_->find(key);
                        if (it == storage_->end()) {
                            message = "-1";
                        } else {
                            std::string stored_value = std::get<0>(it -> second);
                            TimePoint expiry_time = std::get<1>(it->second);

                            if (std::chrono::steady_clock::now() > expiry_time) {
                                storage_->erase(it);
                                message = "-1";
                            } else {
                                message = stored_value;
                            }
                        }       
                    }
                    else {
                        message = "PONG";
                    }

                    write(message, message.size());  // Respond to client
                } else {
                    // Handle errors (including client disconnects)
                    if (ec != asio::error::eof) {
                        std::cerr << "Read error: " << ec.message() << std::endl;
                    }
                }
            });
    }

    void write(std::string data, int length) {
        auto self(shared_from_this());
        std::stringstream msg_stream;
        if (data == "-1") {
            msg_stream << "$-1\r\n";
        } else {
            msg_stream << "$" << length << "\r\n" << data << "\r\n";
        }
        std::string msg = msg_stream.str();
        
        // Async write operation
        asio::async_write(socket_, asio::buffer(msg, msg.size()),
            [this, self](asio::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    read();  // Continue reading after successful write
                } else {
                    std::cerr << "Write error: " << ec.message() << std::endl;
                }
            });
    }

    tcp::socket socket_;          // Client connection socket
    std::array<char, 1024> buffer_;  // Data buffer (fixed-size array)
    std::shared_ptr<StorageType> storage_;
};

void accept_connections(tcp::acceptor& acceptor, std::shared_ptr<StorageType> storage) {
    acceptor.async_accept(
        [&acceptor, storage](asio::error_code ec, tcp::socket socket) {
            if (!ec) {
                std::make_shared<Session>(std::move(socket), storage)->start();
                std::cout << "Client connected" << std::endl;
            }
            accept_connections(acceptor, storage);
        });
}

int main() {
    try {
        asio::io_context io_context;
        
        // Create acceptor listening on port 6379 (IPv4)
        tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), 6379));
        
        auto storage = std::make_shared<StorageType>();  // HEREHEREHERE - Use tuple storage

        // Start accepting connections
        accept_connections(acceptor, storage);
        std::cout << "Server listening on port 6379..." << std::endl;
        
        // Run the I/O service - blocks until all work is done
        io_context.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
