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
using StringStorageType = std::unordered_map<std::string, std::tuple<std::string, TimePoint>>;
using StreamStorageType = std::unordered_map<std::string, std::vector<std::tuple<std::string, std::vector<std::string>>>>;

// Session class handles each client connection. Inherits from enable_shared_from_this to allow safe shared_ptr management in async callbacks
// When shared_from_this() called, a new shared_ptr created. Pointer exists as long as at least one async callback holding it
// When the last shared_ptr destroyed, the Session object will be deleted.
class Session : public std::enable_shared_from_this<Session> {
public:
    Session(
        tcp::socket socket, 
        std::shared_ptr<StringStorageType> storage,
        std::string dir,
        std::string dbfilename,
        std::string masterdetails,
        std::string master_repl_id,
        unsigned master_repl_offset
    ) : socket_(std::move(socket)), string_storage_(storage), dir_(dir), dbfilename_(dbfilename), masterdetails_(masterdetails), master_repl_id_(master_repl_id), master_repl_offset_(master_repl_offset) {}
    void start() {
        read();  // Initiate first read
    }
    void setReplica(bool replica) {
        is_replica_ = replica;
    }

    inline static std::vector<std::shared_ptr<Session>> g_replica_sessions;

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

    // Splits multiple array commands up
    std::vector<std::string> splitMultipleArrayCommands(const std::string &input) {
        std::vector<std::string> tokens;
        size_t pos = 0;
        while (true) {
            // Find an '*' that is immediately followed by a digit.
            size_t start = input.find('*', pos);
            if (start == std::string::npos)
                break;
            if (start + 1 >= input.size() || !std::isdigit(input[start + 1])) {
                // If '*' is not followed by a digit, skip it.
                pos = start + 1;
                continue;
            }
            // Look for the next '*' that is immediately followed by a digit.
            size_t nextPos = start + 1;
            size_t nextDelimiter = std::string::npos;
            while ((nextPos = input.find('*', nextPos)) != std::string::npos) {
                if (nextPos + 1 < input.size() && std::isdigit(input[nextPos + 1])) {
                    nextDelimiter = nextPos;
                    break;
                }
                nextPos++;
            }
            // If no next delimiter is found, take the rest of the input.
            size_t end = (nextDelimiter != std::string::npos) ? nextDelimiter : input.size();
            tokens.push_back(input.substr(start, end - start));
            pos = end;
        }
        return tokens;
    }
    
    // Sends data to replica
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

    void processCommands(std::string data) {
        size_t pos = data.find("\r\n");
        if (pos == std::string::npos) {
            std::cerr << "Invalid data: header not found." << std::endl;
            return;
        }

        // In the event got multiple element of type array in the array
        std::vector<std::string> split_commands = splitMultipleArrayCommands(data);
        for (auto split_command : split_commands) {
            processCommand(split_command);
        }
    }

    // Processes data and adds valid command into array
    // Split data into each data type and their responding data: https://redis.io/docs/latest/develop/reference/protocol-spec/
    void getValidDataTypeChunks(std::string data, std::vector<std::string>& validCommands) {
        size_t pos = data.find("\r\n");
        if (pos == std::string::npos) {
            std::cerr << "Invalid data" << std::endl;
            return;
        }
        // Data is at least ...\r\n
        char token = data[0];
        if (token == '+') {
            size_t fullCommandLength = pos + 2;
            std::string fullCommand = data.substr(0, fullCommandLength);
            validCommands.push_back(fullCommand);
            if (data.size() > fullCommandLength) {
                // Excess data, at least another command within
                std::string remainingData = data.substr(fullCommandLength);
                getValidDataTypeChunks(remainingData, validCommands);
            }
        } else if (token == '$') {
            // Find length of data first
            size_t headerEnd = data.find("\r\n");
            if (headerEnd == std::string::npos) {
                std::cerr << "Incomplete bulk string header. Skipping remainder.\n";
                return;
            }
            std::string lengthStr = data.substr(1, headerEnd - 1);
            int bulkLength = std::stoi(lengthStr);
            
            // Calculate total length: header + CRLF + bulk data + CRLF (if not rdb)
            size_t totalLength;
            if (data.find("REDIS") != std::string::npos) {
                totalLength = headerEnd + 2 + bulkLength; // rdb is like bulk string but without the trailing \r\n
            } else {
                totalLength = headerEnd + 2 + bulkLength + 2;
            }

            if (data.size() < totalLength) {
                std::cout << "Insufficient data, need to get more data" << std::endl;
                // TODO: Instead of throwing an exception, buffer the partial data and wait for additional bytes.
            } else if (data.size() == totalLength) {
                std::string fullCommand = data;
                validCommands.push_back(fullCommand);
            } else {
                std::string fullCommand = data.substr(0, totalLength);
                validCommands.push_back(fullCommand);
                std::string remainingData = data.substr(totalLength);
                // Excess data, at least another command within
                getValidDataTypeChunks(remainingData, validCommands);
            }
        } else if (token == '*') { // For now * used only for commands
            // TODO: Currently assuming no insufficient data or excess
            std::vector<std::string> split_commands = splitMultipleArrayCommands(data);
            for (auto split_command : split_commands) {
                validCommands.push_back(split_command);
            }
        } else {
            std::cout << "New token, not yet handled" << std::endl;
        }
    }

    void processDataByType(std::string command) {
        char token = command[0];
        if (token == '+') {
            std::cout << "Processing data type simple string" << std::endl;
        } else if (token == '$') {
            std::cout << "Processing data type bulk string" << std::endl;
        } else if (token == '*') { // For now * used only for commands
            processCommands(command);
        } else {
            throw std::runtime_error("Unknown token: " + token);
        }
    }

    // Assume command receive is full command, no breaking
    // Data received are either 1) Commands (these are array of bulk strings) 2) RESP type, starts with + * $ etc
    // 1) send from master to replica or client to master 2) server replies or rdb file, not commands
    void read() {
        // https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
        auto self(shared_from_this());
        auto response_buffer = std::make_shared<std::string>();
        asio::async_read_until(
            socket_,
            asio::dynamic_buffer(*response_buffer),
            "\r\n",
            [this, self, response_buffer](asio::error_code ec, std::size_t length) {
                if (!ec) {
                    // Header and leftover not very useful now as data is sent as a whole without breaking
                    // Hence, always has left over which is essentially the part after data
                    std::string header = response_buffer->substr(0, length);
                    std::string leftover = response_buffer->substr(length);     
                    std::string data = header + leftover;
                    std::cout << "Data received: " << data << std::endl;
                    std::vector<std::string> validCommands;

                    getValidDataTypeChunks(data, validCommands);
                    for (auto command : validCommands) {
                        processDataByType(command);
                    }
                    read();
                    
                } else {
                    if (ec != asio::error::eof) {
                        std::cerr << "Read error: " << ec.message() << std::endl;
                    }
                }
            }
        );
    }

    // Validate data is valid command (ie: array of bulk strings) https://redis.io/docs/latest/develop/reference/protocol-spec/#resp-protocol-description
    bool isDataValidRedisCommand(const std::string& data) {
        // Check that data is not empty and starts with '*'
        if (data.empty() || data[0] != '*')
            return false;
        
        // Find end of array header (e.g., "*3\r\n")
        size_t pos = data.find("\r\n");
        if (pos == std::string::npos)
            return false;
        
        // Parse the expected number of elements.
        int expectedElements;
        try {
            expectedElements = std::stoi(data.substr(1, pos - 1));
        } catch (const std::exception&) {
            return false;
        }
        
        // Initialize index after the header.
        size_t index = pos + 2;
        
        for (int i = 0; i < expectedElements; i++) {
            // Ensure the next character exists and is the bulk string marker '$'
            if (index >= data.size() || data[index] != '$')
                return false;
            
            // Find the end of the bulk string header (e.g., "$8\r\n")
            size_t pos2 = data.find("\r\n", index);
            if (pos2 == std::string::npos)
                return false;
            
            // Parse the declared length for this bulk string.
            int bulkLength;
            try {
                bulkLength = std::stoi(data.substr(index + 1, pos2 - index - 1));
            } catch (const std::exception&) {
                return false;
            }
            
            // Calculate the starting index of the bulk data.
            size_t startData = pos2 + 2;
            
            // Check if there are enough bytes for the bulk data plus the trailing "\r\n"
            if (startData + bulkLength + 2 > data.size())
                return false;
            
            index = startData + bulkLength + 2;
        }
        
        // The command is complete only if we have parsed exactly all data.
        return (index == data.size());
    }    

    bool hasAcknowledged(size_t expectedOffset) {
        return lastAcknowledgedBytes >= expectedOffset;
    }

    bool xaadIdIsGreaterThan(const std::string& newId, const std::string& oldId) {
        size_t dashPos_newId = newId.find('-');
        size_t dashPos_oldId = oldId.find('-');
        int leftPart_newId = std::stoi(newId.substr(0, dashPos_newId));
        std::string rightPart_newId = newId.substr(dashPos_newId + 1);
        int leftPart_oldId = std::stoi(oldId.substr(0, dashPos_oldId)); // don't have * so can convert direct to int
        int rightPart_oldId = std::stoi(oldId.substr(dashPos_oldId + 1));

        if (leftPart_newId != leftPart_oldId) {
            return leftPart_newId > leftPart_oldId;
        }
        // If left parts are equal, compare right parts
        // If right part is a wild card then return true
        if (rightPart_newId == "*") {
            return true;
        }
        return std::stoi(rightPart_newId) > rightPart_oldId;
    }

    // Processes commands. Commands are sent in an array consisting of only bulk strings
    void processCommand(const std::string data) {
        if (!isDataValidRedisCommand(data)) {
            std::cout << "Command not of format array of bulk strings: " << data << std::endl;
            return;
        }
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
                (*string_storage_)[key] = std::make_tuple(value, expiry_time);
            } else {
                (*string_storage_)[key] = std::make_tuple(value, TimePoint::max());
            }
            
            if (!is_replica_) { // propagate if not replica and respond
                messages.push_back("OK");
                propagatedCommandSizes += data.size(); 
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

            auto it = string_storage_->find(key);
            if (it == string_storage_->end()) {
            } else {
                std::string stored_value = std::get<0>(it -> second);
                TimePoint expiry_time = std::get<1>(it -> second);
                
                if (std::chrono::system_clock::now() > expiry_time) {
                    string_storage_->erase(it);
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
            for (const auto &entry : *string_storage_) {
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
            if (is_replica_ && split_data[4] == "GETACK") {
                std::string sizeStr = std::to_string(commandFromMasterSizes);
                std::string ackMsg = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + 
                         std::to_string(sizeStr.length()) + "\r\n" + 
                         sizeStr + "\r\n";
                manual_write(ackMsg);
            } else {    
                if (!is_replica_ && split_data[4] == "ACK") {
                    size_t acknowledgedBytes = std::stoull(split_data[6]);
                    this->lastAcknowledgedBytes = acknowledgedBytes;
                } else {
                    // Second Part of handshake with replicas
                    messages.push_back("OK");
                    write(messages, include_size);  
                }
            }
        }
        else if (split_data[2] == "PSYNC") {
            if (!is_replica_) {
                // Third Part of handshake with replicas
                std::string message = "+FULLRESYNC " + master_repl_id_ + " " + std::to_string(master_repl_offset_);
                messages.push_back(message);
                write(messages, include_size);
                g_replica_sessions.push_back(shared_from_this()); // new replica connected
                std::string empty_rdb = "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";
                std::string return_msg = "$" + std::to_string(empty_rdb.length()) + "\r\n" + empty_rdb;
                manual_write(return_msg);
            }
        }
        else if (split_data[2] == "WAIT") {
            int numReplicas = std::stoi(split_data[4]);
            int timeoutMs = std::stoi(split_data[6]);
            if (numReplicas == 0 || timeoutMs == 0) {
                manual_write(":0\r\n");
                return;
            }
            // Create a timer for the timeout
            auto timer = std::make_shared<asio::steady_timer>(socket_.get_executor());
            timer->expires_after(std::chrono::milliseconds(timeoutMs));
        
            // Store a reference to self to keep the session alive
            auto self = shared_from_this();
            
            size_t offset = propagatedCommandSizes;

            if (propagatedCommandSizes > 0) { // Only send GETACK if there’s something to acknowledge
                std::string commandToGetACK = "*3\r\n$8\r\nREPLCONF\r\n$6\r\nGETACK\r\n$1\r\n*\r\n";
                for (auto& replica_session : g_replica_sessions) {
                    if (replica_session) {
                        replica_session->propagate(commandToGetACK);
                    }
                }
                propagatedCommandSizes += commandToGetACK.size();
            }
            // Define a recursive function to check acknowledgments
            std::function<void(const asio::error_code&)> checkAcks;

            checkAcks = [this, self, timer, numReplicas, &checkAcks, offset](const asio::error_code& ec) {
                if (ec) {
                    // Timer was canceled or error occurred
                    manual_write(":0\r\n");
                    return;
                }
                // Count acknowledged replicas
                int replicasAcknowledged = 0;
                for (auto& replica_session : g_replica_sessions) {
                    if (replica_session && replica_session->hasAcknowledged(offset)) {
                        replicasAcknowledged++;
                    }
                }
                
                // Check if we've reached the target or timed out
                if (replicasAcknowledged >= numReplicas || 
                    timer->expiry() <= std::chrono::steady_clock::now()) {
                    // Complete the operation
                    manual_write(":" + std::to_string(replicasAcknowledged) + "\r\n");
                    return;
                }
                    
                // Otherwise, schedule another check after a short delay
                timer->expires_after(std::chrono::milliseconds(50));
                timer->async_wait(checkAcks);
            };
            
            // Start the acknowledgment checking process
            timer->async_wait(checkAcks);            
        }
        else if (split_data[2] == "TYPE") {
            // Get type of data from storage
            std::string key = split_data[4]; // TODO: I think by right a key can only hold one type of data at a time

            auto it_stream = stream_storage_->find(key);
            if (it_stream != stream_storage_->end()) {
                write_simple_string("stream");
            } else {
                auto it_string = string_storage_->find(key);
                if (it_string == string_storage_->end()) {
                    write_simple_string("none");
                } else {
                    std::string stored_value = std::get<0>(it_string -> second);
                    TimePoint expiry_time = std::get<1>(it_string -> second);
                    
                    if (std::chrono::system_clock::now() > expiry_time) {
                        string_storage_->erase(it_string);
                        write_simple_string("none");
                    } else {
                        write_simple_string("string"); 
                    }
                }   
            }
        }
        else if (split_data[2] == "XADD") {
            // Store stream data
            std::string key = split_data[4];
            std::string id = split_data[6];
            std::string leftPart_Id, rightPart_Id;
            std::vector<std::string> subVector(split_data.begin() + 7, split_data.end());
            if (id == "*") {
                std::string generated_id = std::to_string(std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch()).count());
                leftPart_Id = generated_id;
                rightPart_Id = "0";
                id = leftPart_Id + "-" + rightPart_Id;
            } else {
                size_t dashPos_Id = id.find('-');
                leftPart_Id = id.substr(0, dashPos_Id);
                rightPart_Id = id.substr(dashPos_Id + 1);

            }

            auto it_stream = stream_storage_->find(key);
            if (it_stream == stream_storage_->end()) {
                // create new entry in dictionary
                std::vector<std::tuple<std::string, std::vector<std::string>>> new_entry;
                if (leftPart_Id == "0" && rightPart_Id == "*") {
                    id = leftPart_Id + "-" + "1"; // since lowest is 0-1
                } else if (rightPart_Id == "*") {
                    id = leftPart_Id + "-" + "0"; // default sequence number is 0
                }
                new_entry.push_back(std::make_tuple(id, subVector));
                (*stream_storage_)[key] = new_entry;
                write_bulk_string(id);
            } else {
                // check if entry is valid
                const auto& last_tuple = it_stream->second.back();
                std::string id_last_entry = std::get<0>(last_tuple);
                if (!xaadIdIsGreaterThan(id, "0-0")) {
                    manual_write("-ERR The ID specified in XADD must be greater than 0-0\r\n");
                } else if (!xaadIdIsGreaterThan(id, id_last_entry)) {
                    manual_write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n");
                } else {
                    if (rightPart_Id == "*") {
                        size_t dashPos_id_last_entry = id_last_entry.find('-');
                        std::string leftPart_Id_last_entry = id_last_entry.substr(0, dashPos_id_last_entry);
                        int rightPart_Id_last_entry = std::stoi(id_last_entry.substr(dashPos_id_last_entry + 1));
                        rightPart_Id_last_entry += 1;

                        if (std::stoi(leftPart_Id) > std::stoi(leftPart_Id_last_entry)) {
                            // if right side bigger and even if right part is *, just go to the new sequence with x-0
                            id = leftPart_Id + "-" + "0";
                        } else {
                            id = leftPart_Id_last_entry + "-" + std::to_string(rightPart_Id_last_entry);
                        }
                    }
                    auto& vector_of_tuples = it_stream->second;
                    vector_of_tuples.push_back(std::make_tuple(id, subVector));
                    write_bulk_string(id);
                }
            }
        }
        else {
            if (!is_replica_) {
                messages.push_back("PONG");
                write(messages, include_size);  
            }
        }
        // Adds commands received so far
        commandFromMasterSizes += data.size();
    }

    // TODO: Write in different format simple string, bulk string, array
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

    void write_simple_string(std::string message) {
        auto self(shared_from_this());
        std::string formatted_message = "+" + message + "\r\n";
        std::cout << "MESSAGE SENT (simple string)..: " << formatted_message << std::endl;
        asio::async_write(socket_, asio::buffer(formatted_message, formatted_message.size()),
            [this, self](asio::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    read();  // Continue reading after successful write
                } else {
                    std::cerr << "Write error: " << ec.message() << std::endl;
                }
            });
    }

    void write_bulk_string(std::string message) {
        auto self(shared_from_this());
        std::string formatted_message = "$" + std::to_string(message.size()) + "\r\n" + message + "\r\n";
        std::cout << "MESSAGE SENT (bulk string)..: " << formatted_message << std::endl;
        asio::async_write(socket_, asio::buffer(formatted_message, formatted_message.size()),
            [this, self](asio::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    read();  // Continue reading after successful write
                } else {
                    std::cerr << "Write error: " << ec.message() << std::endl;
                }
            });
    }

    // Write with formatting, adding size of all messages and size of individual message
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
    std::shared_ptr<StringStorageType> string_storage_;
    std::shared_ptr<StreamStorageType> stream_storage_ = std::make_shared<StreamStorageType>();;
    std::string dir_;
    std::string dbfilename_;
    std::string masterdetails_;
    std::string master_repl_id_;
    unsigned master_repl_offset_;
    bool is_replica_ = false;
    size_t commandFromMasterSizes = 0;
    size_t propagatedCommandSizes = 0;
    size_t lastAcknowledgedBytes = 0;
};

void accept_connections(
        tcp::acceptor& acceptor, 
        std::shared_ptr<StringStorageType> storage,
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
                     std::shared_ptr<StringStorageType> storage,
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
                                            std::string second_replconf = "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n";
                                            asio::async_write(
                                                *master_socket,
                                                asio::buffer(second_replconf),
                                                [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset](asio::error_code ec, std::size_t /*length*/) {
                                                    if (!ec) {
                                                        readResponse(master_socket, "after REPLCONF", [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset]() {
                                                            // THIRD STEP SEND PSYNC
                                                            std::string psync = "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n";
                                                            asio::async_write(
                                                                *master_socket,
                                                                asio::buffer(psync),
                                                                [master_socket, storage, dir, dbfilename, masterdetails, master_repl_id, master_repl_offset](asio::error_code ec, std::size_t /*length*/) {
                                                                    if (!ec) {
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
