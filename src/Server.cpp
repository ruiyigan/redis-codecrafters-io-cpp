#include <iostream>
#include <memory>       
#include <asio.hpp>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <chrono>
#include <fstream>  
#include <filesystem>
#include "../include/storage.hpp"
#include "../include/session.hpp"

using namespace redis_server;

using asio::ip::tcp;  // Keep this where tcp is used

void accept_connections(
        tcp::acceptor& acceptor, 
        std::shared_ptr<StringStorageType> storage,
        std::string dir,
        std::string dbfilename,
        std::string masterdetails,
        std::string master_repl_id,
        unsigned master_repl_offset,
        std::shared_ptr<StreamStorageType> stream_storage
    ) {
    acceptor.async_accept(
        [&acceptor, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, stream_storage](asio::error_code ec, tcp::socket socket) {
            if (!ec) {
                std::make_shared<Session>(std::move(socket), storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, stream_storage)->start();
                std::cout << "Client connected" << std::endl;
                
            }
            accept_connections(acceptor, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, stream_storage); // Recursively continues to listen for new connections 
        });
}

// Helper function to parse master details
std::pair<std::string, std::string> parseHostPort(const std::string& masterdetails) {
    std::istringstream iss(masterdetails);
    std::string host, port;
    iss >> host >> port;
    return {host, port};
}

// Helper to read until "\r\n" and then call a callback
void readResponse(std::shared_ptr<tcp::socket> socket, const std::string& context, std::function<void()> callback) {
    auto response_buffer = std::make_shared<std::string>();
    asio::async_read_until(
        *socket,
        asio::dynamic_buffer(*response_buffer),
        "\r\n",
        [socket, response_buffer, context, callback](asio::error_code ec, std::size_t length) {
            if (!ec) {
                std::string response = response_buffer->substr(0, length);
                std::cout << "Received from master " << context << ": " << response << std::endl;
                callback();
            } else {
                std::cerr << "Error reading response " << context << ": " << ec.message() << std::endl;
            }
        }
    );
}

// Helper to perform handshake and establish connection with master by a replica
void connectToMaster(asio::io_context& io_context, 
                     const std::string& masterdetails,
                     std::shared_ptr<StringStorageType> storage,
                     const std::string& dir,
                     const std::string& dbfilename,
                     const std::string& master_repl_id,
                     unsigned master_repl_offset,
                     unsigned portnumber,
                     std::shared_ptr<StreamStorageType> stream_storage) {
    auto [masterHost, masterPort] = parseHostPort(masterdetails);

    auto master_socket = std::make_shared<tcp::socket>(io_context);
    tcp::resolver resolver(io_context);
    auto endpoints = resolver.resolve(masterHost, masterPort);

    asio::async_connect(
        *master_socket,
        endpoints,
        [master_socket, &io_context, masterdetails, storage, dir, dbfilename, master_repl_id, master_repl_offset, portnumber, stream_storage](asio::error_code ec, tcp::endpoint /*ep*/) {
            if (!ec) {
                std::cout << "Connected to master. Now sending PING..." << std::endl;
                // FIRST STEP SEND PING
                std::string ping_cmd = "*1\r\n$4\r\nPING\r\n";
                asio::async_write(
                    *master_socket,
                    asio::buffer(ping_cmd),
                    [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, portnumber, stream_storage](asio::error_code ec, std::size_t /*length*/) {
                        if (!ec) {
                            readResponse(master_socket, "after PING", [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, portnumber, stream_storage]() {
                                // SECOND STEP SEND REPLCONF commands
                                std::string port_str = std::to_string(portnumber);
                                std::string first_replconf = "*3\r\n"
                                                            "$8\r\nREPLCONF\r\n"
                                                            "$14\r\nlistening-port\r\n"
                                                            "$" + std::to_string(port_str.size()) + "\r\n" +
                                                            port_str + "\r\n";
                                asio::async_write(
                                    *master_socket,
                                    asio::buffer(first_replconf),
                                    [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, stream_storage](asio::error_code ec, std::size_t /*length*/) {
                                        if (!ec) {
                                            std::string second_replconf = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                                            asio::async_write(
                                                *master_socket,
                                                asio::buffer(second_replconf),
                                                [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, stream_storage](asio::error_code ec, std::size_t /*length*/) {
                                                    if (!ec) {
                                                        readResponse(master_socket, "after REPLCONF", [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, stream_storage]() {
                                                            // THIRD STEP SEND PSYNC
                                                            std::string psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                                                            asio::async_write(
                                                                *master_socket,
                                                                asio::buffer(psync),
                                                                [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, stream_storage](asio::error_code ec, std::size_t /*length*/) {
                                                                    if (!ec) {
                                                                        readResponse(master_socket, "after PSYNC", [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, stream_storage]() {
                                                                            std::cout << "Replication handshake complete. Switching to replica session." << std::endl;
                                                                            // Now, wrap the master_socket in a Session with replica mode enabled.
                                                                            auto replica_session = std::make_shared<Session>(
                                                                                std::move(*master_socket),
                                                                                storage,
                                                                                dir,
                                                                                dbfilename,
                                                                                masterdetails,
                                                                                master_repl_id,
                                                                                master_repl_offset,
                                                                                stream_storage                        
                                                                            );
                                                                            replica_session->setReplica(true);
                                                                            replica_session->start();
                                                                        });
                                                                    } else {
                                                                        std::cerr << "Error sending PSYNC to master: " << ec.message() << std::endl;
                                                                    }
                                                                }
                                                            );
                                                        });
                                                    } else {
                                                        std::cerr << "Error sending second REPLCONF to master: " << ec.message() << std::endl;
                                                    }
                                                }
                                            );
                                        } else {
                                            std::cerr << "Error sending first REPLCONF to master: " << ec.message() << std::endl;
                                        }
                                    }
                                );
                            });
                        } else {
                            std::cerr << "Error sending PING to master: " << ec.message() << std::endl;
                        }
                    }
                );
            } else {
                std::cerr << "Error connecting to master: " << ec.message() << std::endl;
            }
        }
    );
}

// Helper to read size based on RDB format
uint64_t readDecodedSize(std::ifstream &file) {
    char first_byte;
    file.get(first_byte);
    unsigned char unsigned_first_byte = static_cast<unsigned char>(first_byte);
    int type = (first_byte & 0xC0) >> 6;
    uint64_t length = 0;

    if (type == 0) {
        length = unsigned_first_byte & 0x3F; // 00111111
    } else if (type == 1) {
        char second_byte;
        file.get(second_byte);
        length = unsigned_first_byte & 0x3F + static_cast<unsigned char>(second_byte);
    } else if (type == 2) {
        unsigned char buf[4];
        file.read(reinterpret_cast<char*>(buf), 4);
        length = (uint64_t)buf[0]
            | ((uint64_t)buf[1] << 24)
            | ((uint64_t)buf[2] << 16)
            | ((uint64_t)buf[3] << 8);
    } else {
        length = 0; // string encoding, ignored for now
    }
    return length;
}

// Helper to read expiry based on RDB format
TimePoint readExpiry(std::ifstream &file, unsigned char umarker) {
    TimePoint expiry = TimePoint::max();
    if (umarker == 0xFC) {
        // Read 8-byte expiry (milliseconds)
        unsigned char buff[8];
        file.read(reinterpret_cast<char*>(buff), 8);
        auto expiry_ms = (uint64_t)buff[0]
                        | ((uint64_t)buff[1] << 8)
                        | ((uint64_t)buff[2] << 16)
                        | ((uint64_t)buff[3] << 24)
                        | ((uint64_t)buff[4] << 32)
                        | ((uint64_t)buff[5] << 40)
                        | ((uint64_t)buff[6] << 48)
                        | ((uint64_t)buff[7] << 56);
        expiry = std::chrono::system_clock::time_point(std::chrono::milliseconds(expiry_ms));
    } else if (umarker == 0xFD) {
        // Read 4-byte expiry (seconds)
        unsigned char buff[4];
        file.read(reinterpret_cast<char*>(buff), 4);
        auto expiry_s = (uint64_t)buff[0]
                        | ((uint64_t)buff[1] << 8)
                        | ((uint64_t)buff[2] << 16)
                        | ((uint64_t)buff[3] << 24);
        expiry = std::chrono::system_clock::time_point(std::chrono::seconds(expiry_s));
    } else {
        // If no valid expiry marker is found, you might decide to push the marker back,
        // or simply assume no expiry. Here, we simply treat it as "no expiry".
        file.unget();
    }
    return expiry;
}

// Helper to read string based on RDB format
std::string readString(std::ifstream &file) {
    int size = readDecodedSize(file);
    std::vector<char> buffer(size);
    file.read(buffer.data(), size);
    return std::string(buffer.data(), size);
}

void loadDatabase(const std::string &dir, const std::string &dbfilename, std::shared_ptr<StringStorageType> storage) {
    std::string filepath = dir + "/" + dbfilename;
    if (!std::filesystem::exists(filepath)) {
        std::cerr << "File does not exist: " << filepath << std::endl;
        return;
    }
    std::ifstream file(filepath, std::ios::binary);
    if (!file.is_open()) {
        throw std::runtime_error("Could not open file: " + filepath);
    }

    char ch;
    uint64_t size;
    uint64_t size_with_expiry;
    bool is_database = false;
    while (file.get(ch)) {
        unsigned char byte = static_cast<unsigned char>(ch);
        if (byte == 0xFB && !is_database) {
            size = readDecodedSize(file);
            size_with_expiry = readDecodedSize(file);
            is_database = true;
            continue;
        }

        if (is_database) { // Process the database section
            TimePoint expiry_time = TimePoint::max();
            if (static_cast<unsigned char>(ch) == 0xFC) {
                expiry_time = readExpiry(file, 0xFC);
                file.get(ch);
            } else if (static_cast<unsigned char>(ch) == 0xFD) {
                expiry_time = readExpiry(file, 0xFD);
                file.get(ch);
            }

            if (static_cast<unsigned char>(ch) == 0x00) {
                std::string key = readString(file);
                if (key.empty()) {
                    break;
                }
                std::string value = readString(file);
                (*storage)[key] = std::make_tuple(value, expiry_time);
            }
        }
    }
}

int main(int argc, char* argv[]) {
    try {
        asio::io_context io_context;
        std::string dir;
        std::string dbfilename;
        unsigned portnumber = 6379;
        std::string masterdetails = "";
        std::string master_repl_id = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb";
        unsigned master_repl_offset = 0;

        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];

            if (arg == "--dir") {
                dir = argv[i + 1];
            }

            if (arg == "--dbfilename") {
                dbfilename = argv[i + 1];
            }

            if (arg == "--port") {
                portnumber = std::stoi(argv[i + 1]);
            }

            if (arg == "--replicaof") {
                masterdetails = argv[i + 1];
            }
        }
        
        // Create acceptor listening on port 6379 if not specified
        tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), portnumber));
        
        auto storage = std::make_shared<StringStorageType>();  // Use tuple storage
        auto stream_storage = std::make_shared<StreamStorageType>();
        loadDatabase(dir, dbfilename, storage);

        // Start accepting connections
        accept_connections(acceptor, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, stream_storage);
        std::cout << "Server listening on port " << portnumber << "..." << std::endl;

        if (!masterdetails.empty()) {
            connectToMaster(io_context, masterdetails, storage, dir, dbfilename, master_repl_id, master_repl_offset, portnumber, stream_storage);
        }
        
        // Run the I/O service - blocks until all work is done
        io_context.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
