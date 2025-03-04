#include <iostream>
#include <memory>       
#include <asio.hpp>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <chrono>
#include <fstream>  
#include <filesystem>

// Use of type aliasing
using asio::ip::tcp;  
using TimePoint = std::chrono::system_clock::time_point;
using StorageType = std::unordered_map<std::string, std::tuple<std::string, TimePoint>>;

// Session class handles each client connection. Inherits from enable_shared_from_this to allow safe shared_ptr management in async callbacks
// When shared_from_this() called, a new shared_ptr created. Pointer exists as long as at least one async callback holding it
// When the last shared_ptr destroyed, the Session object will be deleted.

class Session;  // Forward declaration
std::vector<std::shared_ptr<Session>> g_replica_sessions;

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(
        tcp::socket socket, 
        std::shared_ptr<StorageType> storage,
        std::string dir,
        std::string dbfilename,
        std::string masterdetails,
        std::string master_repl_id,
        unsigned master_repl_offset
    ) : socket_(std::move(socket)), storage_(storage), dir_(dir), dbfilename_(dbfilename), masterdetails_(masterdetails), master_repl_id_(master_repl_id), master_repl_offset_(master_repl_offset) {}
    void start() {
        read();  // Initiate first read
    }
    void setReplica(bool replica) {
        is_replica_ = replica;
    }

    // static std::shared_ptr<Session> g_replica_session;

private:
    // Helper function to split string based on delimiter provided
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

    std::vector<std::string> splitRedisCommands(const std::string &input) {
        std::vector<std::string> tokens;
        std::stringstream stream(input);
        std::string token;
    
        // Split based on '*' character
        while (std::getline(stream, token, '*')) {
            if (!token.empty()) {
                if (token.back() == '\r')
                    token.pop_back();
                // Check if token begins with a digit (this is a naive check to ensure it's a command)
                if (!token.empty() && std::isdigit(token[0])) {
                    tokens.push_back("*" + token);
                }
            }
        }
        return tokens;
    }
    
    void propagate(const std::string &command) {
        auto self(shared_from_this());
        asio::async_write(socket_, asio::buffer(command),
            [this, self](asio::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    read();
                } else {
                    std::cerr << "Error propagating command: " << ec.message() << std::endl;
                }
            });
    }

    void processData(const std::string data) {
        std::vector<std::string> split_data = splitString(data, '\n');
        std::vector<std::string> messages;
        bool include_size = false;
        if (split_data[2] == "ECHO") {
            // Echos back message
            messages.push_back(split_data.back());
            write(messages, include_size);  
        }
        else if (split_data[2] == "SET") {
            // Saves data from user
            std::string key = split_data[4];
            std::string value = split_data[6];
            std::time_t expiry_time = 0;
            if (split_data.size() >= 11 && split_data[8] == "px") {
                int expiry_ms = std::stoi(split_data[10]); // milliseconds
                auto expiry_time = std::chrono::system_clock::now() + std::chrono::milliseconds(expiry_ms);
                (*storage_)[key] = std::make_tuple(value, expiry_time);
            } else {
                (*storage_)[key] = std::make_tuple(value, TimePoint::max());
            }
            
            if (!is_replica_) { // propagate if not replica and respond
                messages.push_back("OK");
                std::cout << "PROPGATING Following Data: " << data << std::endl;
                for (auto& replica_session : g_replica_sessions) {
                    if (replica_session) {
                        replica_session->propagate(data);
                    }
                }
                write(messages, include_size);  
            } else { // continue to read for replicas
                read();
            }
        } 
        else if (split_data[2] == "GET")
        {
            // Get data from storage
            std::string key = split_data[4];

            auto it = storage_->find(key);
            if (it == storage_->end()) {
            } else {
                std::string stored_value = std::get<0>(it -> second);
                TimePoint expiry_time = std::get<1>(it -> second);
                
                std::cout << "TIME NOW at expiry FROM RDB..: " << expiry_time.time_since_epoch().count() << std::endl;
                std::cout << "TIME NOW..: " << std::chrono::system_clock::now().time_since_epoch().count() << std::endl;
                if (std::chrono::system_clock::now() > expiry_time) {
                    storage_->erase(it);
                } else {
                    messages.push_back(stored_value);
                }
            }   
            write(messages, include_size);      
        }
        else if (split_data[2] == "CONFIG") 
        {
            // Get config details
            if (split_data[4] == "GET") {
                std::string param_name = split_data[6];
                std::string param_value = dir_;
                messages.push_back(param_name);
                messages.push_back(param_value);
            }
            write(messages, include_size);  
        }
        else if (split_data[2] == "KEYS") {
            // Get keys of redis
            for (const auto &entry : *storage_) {
                messages.push_back(entry.first);
            }
            include_size = true;
            write(messages, include_size);  
        }
        else if (split_data[2] == "INFO") {
            if (masterdetails_ == "") {
                // Master
                std::string role = "role:master";
                std::string master_repl_offset = "nmaster_repl_offset:0";
                std::string nmaster_replid = "nmaster_replid:";
                nmaster_replid += master_repl_id_;
                std::string message = role + "\r\n" + master_repl_offset + "\r\n" + nmaster_replid;
                messages.push_back(message);
            } else {
                // Not Master
                messages.push_back("role:slave");
            }
            write(messages, include_size);  
        }
        else if (split_data[2] == "REPLCONF") {
            // Second Part of handshake with replicas
            messages.push_back("OK");
            write(messages, include_size);  
        }
        else if (split_data[2] == "PSYNC") {
            // Third Part of handshake with replicas
            std::string message = "+FULLRESYNC " + master_repl_id_ + " " + std::to_string(master_repl_offset_);
            messages.push_back(message);
            write(messages, include_size);

            g_replica_sessions.push_back(shared_from_this());

            std::string empty_rdb = "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";
            std::string return_msg = "$" + std::to_string(empty_rdb.length()) + "\r\n" + empty_rdb;
            manual_write(return_msg);
        }
        else {
            messages.push_back("PONG");
            write(messages, include_size);  
        }
    }

    void read() {
        // Capture a shared_ptr to keep object alive during async operation, all shared pointer shares the same reference count
        auto self(shared_from_this());
        
        socket_.async_read_some(asio::buffer(buffer_),
            // Lambda captures 'this' and self (shared_ptr) for proper lifetime
            [this, self](asio::error_code ec, std::size_t length) {
                if (!ec) {
                    std::string data = std::string(buffer_.data(), length);
                    std::cout << "Received: \n" << data << std::endl;
                    std::cout << "Received first char: \n" << data[0] << std::endl;
                    if (data[0] == '*') {
                        // Split multiple commands into individual command
                        std::vector<std::string> split_commands = splitRedisCommands(data);
                        for (auto split_command : split_commands) {
                            // std::cout << "COMMAND RECEIVED: \n" << split_command << std::endl;
                            processData(split_command);
                        }
                    } else {
                        processData(data);
                    }
                } else {
                    // Handle errors (including client disconnects)
                    if (ec != asio::error::eof) {
                        std::cerr << "Read error: " << ec.message() << std::endl;
                    }
                }
            });
    }

    // Write without any parsing
    void manual_write(std::string message) {
        auto self(shared_from_this());
        std::cout << "MESSAGE SENT (manual)..: " << message << std::endl;
        asio::async_write(socket_, asio::buffer(message, message.size()),
            [this, self](asio::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    read();  // Continue reading after successful write
                } else {
                    std::cerr << "Write error: " << ec.message() << std::endl;
                }
            });
    }
    void write(std::vector<std::string> messages, bool size = false) {
        // Similar to read function, creates a shared pointer
        auto self(shared_from_this());
        std::stringstream msg_stream;

        if (messages.size() == 0) {
            msg_stream << "$-1\r\n";
        } else {
            int num_messages = messages.size();
            if (messages.size() > 1 || size) {
                msg_stream << "*" << num_messages << "\r\n";
            }
            for (std::string message: messages) {
                msg_stream << "$" << message.size() << "\r\n" << message << "\r\n";
            }
        }
        std::string msg = msg_stream.str();
        std::cout << "MESSAGE SENT..: " << msg << std::endl;
        asio::async_write(socket_, asio::buffer(msg, msg.size()),
            [this, self](asio::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    read();  // Continue reading after successful write
                } else {
                    std::cerr << "Write error: " << ec.message() << std::endl;
                }
            });
    }

    // Attributes of Session class
    tcp::socket socket_;          
    std::array<char, 1024> buffer_;  
    std::shared_ptr<StorageType> storage_;
    std::string dir_;
    std::string dbfilename_;
    std::string masterdetails_;
    std::string master_repl_id_;
    unsigned master_repl_offset_;
    bool is_replica_ = false;
};

void accept_connections(
        tcp::acceptor& acceptor, 
        std::shared_ptr<StorageType> storage,
        std::string dir,
        std::string dbfilename,
        std::string masterdetails,
        std::string master_repl_id,
        unsigned master_repl_offset
    ) {
    acceptor.async_accept(
        [&acceptor, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset](asio::error_code ec, tcp::socket socket) {
            if (!ec) {
                std::make_shared<Session>(std::move(socket), storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset)->start();
                std::cout << "Client connected" << std::endl;
                
            }
            accept_connections(acceptor, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset); // Recursively continues to listen for new connections 
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
                     std::shared_ptr<StorageType> storage,
                     const std::string& dir,
                     const std::string& dbfilename,
                     const std::string& master_repl_id,
                     unsigned master_repl_offset,
                     unsigned portnumber) {
    auto [masterHost, masterPort] = parseHostPort(masterdetails);

    auto master_socket = std::make_shared<tcp::socket>(io_context);
    tcp::resolver resolver(io_context);
    auto endpoints = resolver.resolve(masterHost, masterPort);

    asio::async_connect(
        *master_socket,
        endpoints,
        [master_socket, &io_context, masterdetails, storage, dir, dbfilename, master_repl_id, master_repl_offset, portnumber](asio::error_code ec, tcp::endpoint /*ep*/) {
            if (!ec) {
                std::cout << "Connected to master. Now sending PING..." << std::endl;
                // FIRST STEP SEND PING
                std::string ping_cmd = "*1\r\n$4\r\nPING\r\n";
                asio::async_write(
                    *master_socket,
                    asio::buffer(ping_cmd),
                    [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, portnumber](asio::error_code ec, std::size_t /*length*/) {
                        if (!ec) {
                            std::cout << "PING command sent successfully to master!" << std::endl;
                            readResponse(master_socket, "after PING", [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset, portnumber]() {
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
                                    [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset](asio::error_code ec, std::size_t /*length*/) {
                                        if (!ec) {
                                            std::cout << "first_replconf command sent successfully to master!" << std::endl;
                                            std::string second_replconf = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                                            asio::async_write(
                                                *master_socket,
                                                asio::buffer(second_replconf),
                                                [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset](asio::error_code ec, std::size_t /*length*/) {
                                                    if (!ec) {
                                                        std::cout << "second_replconf command sent successfully to master!" << std::endl;
                                                        readResponse(master_socket, "after REPLCONF", [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset]() {
                                                            // THIRD STEP SEND PSYNC
                                                            std::string psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                                                            asio::async_write(
                                                                *master_socket,
                                                                asio::buffer(psync),
                                                                [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset](asio::error_code ec, std::size_t /*length*/) {
                                                                    if (!ec) {
                                                                        std::cout << "PSYNC command sent successfully to master!" << std::endl;
                                                                        readResponse(master_socket, "after PSYNC", [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset]() {
                                                                            std::cout << "Replication handshake complete. Switching to replica session." << std::endl;
                                                                            // Now, wrap the master_socket in a Session with replica mode enabled.
                                                                            auto replica_session = std::make_shared<Session>(
                                                                                std::move(*master_socket),
                                                                                storage,
                                                                                dir,
                                                                                dbfilename,
                                                                                masterdetails,
                                                                                master_repl_id,
                                                                                master_repl_offset
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


void loadDatabase(const std::string &dir, const std::string &dbfilename, std::shared_ptr<StorageType> storage) {
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
                std::cout << "Loaded key: " << key << ", value: " << value << std::endl;
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
        
        auto storage = std::make_shared<StorageType>();  // Use tuple storage
        loadDatabase(dir, dbfilename, storage);

        // Start accepting connections
        accept_connections(acceptor, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset);
        std::cout << "Server listening on port " << portnumber << "..." << std::endl;

        if (!masterdetails.empty()) {
            connectToMaster(io_context, masterdetails, storage, dir, dbfilename, master_repl_id, master_repl_offset, portnumber);
        }
        
        // Run the I/O service - blocks until all work is done
        io_context.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
