#include <iostream>
#include <memory>       // For smart pointers and enable_shared_from_this
#include <asio.hpp>
#include <vector>
#include <sstream>
#include <unordered_map>
#include <chrono>
#include <fstream>  // Include fstream for file operations
#include <filesystem>

using asio::ip::tcp;  
using TimePoint = std::chrono::system_clock::time_point;
using StorageType = std::unordered_map<std::string, std::tuple<std::string, TimePoint>>;

// Session handles each client connection. Inherits from enable_shared_from_this
// to allow safe shared_ptr management in async callbacks
class Session : public std::enable_shared_from_this<Session> {
public:
    // Constructor takes ownership of the socket
    Session(
        tcp::socket socket, 
        std::shared_ptr<StorageType> storage,
        std::string dir,
        std::string dbfilename
    ) : socket_(std::move(socket)), storage_(storage), dir_(dir), dbfilename_(dbfilename) {}
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

    // helper to read size
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
            length = 0; // string encoding
        }
        return length;
    }
    
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

    std::string readString(std::ifstream &file) {
        int size = readDecodedSize(file);
        std::vector<char> buffer(size);
        file.read(buffer.data(), size);
        return std::string(buffer.data(), size);
    }

    void readFile(const std::string& dir, const std::string& filename, std::shared_ptr<StorageType> storage_) {
        std::string filepath = dir + "/" + filename;

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

            if (is_database) { // means reached database section
                TimePoint expiry_time = TimePoint::max();
                if ((static_cast<unsigned char>(ch) == 0xFC)) {
                    expiry_time = readExpiry(file, 0xFC);
                    file.get(ch);
                } else if ((static_cast<unsigned char>(ch) == 0xFD)) {
                    expiry_time = readExpiry(file, 0xFD);
                    file.get(ch);
                }

                if (static_cast<unsigned char>(ch) == 0x00) {
                    std::string key = readString(file);
                    std::string value = readString(file);   
                    std::cout << "KEY FROM RDB..: " << key << std::endl;
                    std::cout << "VALUE FROM RDB..: " << value << std::endl;
                    std::cout << "EXPIRY FROM RDB..: " << expiry_time.time_since_epoch().count() << std::endl;
                    (*storage_)[key] = std::make_tuple(value, expiry_time);
                } 
            }
            
        }
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

                    std::vector<std::string> messages;
                    readFile(dir_, dbfilename_, storage_);
                    bool include_size = false;
                    if (split_data[2] == "ECHO") {
                        // repeat
                        messages.push_back(split_data.back());
                    }
                    else if (split_data[2] == "SET") {
                        // save
                        std::string key = split_data[4];
                        std::string value = split_data[6];
                        std::time_t expiry_time = 0;
                        if (split_data.size() >= 11 && split_data[8] == "px") {
                            int expiry_ms = std::stoi(split_data[10]); // milliseconds
                            auto expiry_time = std::chrono::system_clock::now() + std::chrono::milliseconds(expiry_ms);
                            (*storage_)[key] = std::make_tuple(value, expiry_time);
                        } else {
                            // Use a distant future time if no expiry is specified
                            (*storage_)[key] = std::make_tuple(value, TimePoint::max());
                        }
                        messages.push_back("OK");
                    } 
                    else if (split_data[2] == "GET")
                    {
                        // get
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
                    }
                    else if (split_data[2] == "CONFIG") {
                        if (split_data[4] == "GET") {
                            std::string param_name = split_data[6];
                            std::string param_value = dir_;
                            messages.push_back(param_name);
                            messages.push_back(param_value);
                        }
                    }
                    else if (split_data[2] == "KEYS") {
                        for (const auto &entry : *storage_) {
                            messages.push_back(entry.first);
                        }
                        include_size = true;
                    }
                    else {
                        messages.push_back("PONG");
                    }

                    write(messages, include_size);  // Respond to client
                } else {
                    // Handle errors (including client disconnects)
                    if (ec != asio::error::eof) {
                        std::cerr << "Read error: " << ec.message() << std::endl;
                    }
                }
            });
    }

    void write(std::vector<std::string> messages, bool size = false) {
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
        

        std::cout << "MESSAGE..: " << msg << std::endl;
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
    std::shared_ptr<StorageType> storage_; // shared acrosss sessions
    std::string dir_;
    std::string dbfilename_;
};

void accept_connections(
        tcp::acceptor& acceptor, 
        std::shared_ptr<StorageType> storage,
        std::string dir,
        std::string dbfilename
    ) {
    acceptor.async_accept(
        [&acceptor, storage, dir, dbfilename](asio::error_code ec, tcp::socket socket) {
            if (!ec) {
                std::make_shared<Session>(std::move(socket), storage, dir, dbfilename)->start();
                std::cout << "Client connected" << std::endl;
            }
            accept_connections(acceptor, storage, dir, dbfilename); // recursion
        });
}

int main(int argc, char* argv[]) {
    try {
        asio::io_context io_context;
        std::string dir;
        std::string dbfilename;

        for (int i = 1; i < argc; ++i) {
            std::string arg = argv[i];

            if (arg == "--dir") {
                dir = argv[i + 1];
            }

            if (arg == "--dbfilename") {
                dbfilename = argv[i + 1];
            }
        }
        
        // Create acceptor listening on port 6379 (IPv4)
        tcp::acceptor acceptor(io_context, tcp::endpoint(tcp::v4(), 6379));
        
        auto storage = std::make_shared<StorageType>();  // Use tuple storage

        // Start accepting connections
        accept_connections(acceptor, storage, dir, dbfilename);
        std::cout << "Server listening on port 6379..." << std::endl;
        
        // Run the I/O service - blocks until all work is done
        io_context.run();
    } catch (std::exception& e) {
        std::cerr << "Exception: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}
