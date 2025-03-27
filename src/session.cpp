#include "../include/session.hpp"
#include <iostream>
#include <sstream>
#include <functional>

using asio::ip::tcp; 
namespace redis_server {

Session::Session(
    asio::ip::tcp::socket socket, 
    std::shared_ptr<StringStorageType> storage,
    std::string dir,
    std::string dbfilename,
    std::string masterdetails,
    std::string master_repl_id,
    unsigned master_repl_offset,
    std::shared_ptr<StreamStorageType> stream_storage
) : socket_(std::move(socket)), 
    string_storage_(storage), 
    dir_(dir), 
    dbfilename_(dbfilename), 
    masterdetails_(masterdetails), 
    master_repl_id_(master_repl_id), 
    master_repl_offset_(master_repl_offset), 
    stream_storage_(stream_storage) {}

void Session::start() {
    read();
}

void Session::setReplica(bool replica) {
    is_replica_ = replica;
}

std::vector<std::string> Session::splitString(const std::string& input, char delimiter) {
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
std::vector<std::string> Session::splitMultipleArrayCommands(const std::string &input) {
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
void Session::propagate(const std::string &command) {
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

void Session::processCommands(std::string data) {
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
void Session::getValidDataTypeChunks(std::string data, std::vector<std::string>& validCommands) {
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

void Session::processDataByType(std::string command) {
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
void Session::read() {
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
bool Session::isDataValidRedisCommand(const std::string& data) {
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

bool Session::hasAcknowledged(size_t expectedOffset) {
    return lastAcknowledgedBytes >= expectedOffset;
}

bool Session::xaadIdIsGreaterThan(const std::string& newId, const std::string& oldId) {
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

bool Session::xaadIdIsLessThan(const std::string& newId, const std::string& oldId) {
    if (oldId == "+") {
        return true; // means max, always take till the end of the stream
    }
    size_t dashPos_newId = newId.find('-');
    size_t dashPos_oldId = oldId.find('-');
    int leftPart_newId = std::stoi(newId.substr(0, dashPos_newId));
    std::string rightPart_newId = newId.substr(dashPos_newId + 1);
    int leftPart_oldId = std::stoi(oldId.substr(0, dashPos_oldId)); // don't have * so can convert direct to int
    int rightPart_oldId = std::stoi(oldId.substr(dashPos_oldId + 1));

    if (leftPart_newId != leftPart_oldId) {
        return leftPart_newId < leftPart_oldId;
    }
    // If left parts are equal, compare right parts
    // If right part is a wild card then return true
    if (rightPart_newId == "*") {
        return false;
    }
    return std::stoi(rightPart_newId) < rightPart_oldId;
}

// Helper function to get stream entries that match criteria
std::pair<int, std::string> Session::getStreamEntries(
    const std::string& key,
    const std::string& start_id,
    const std::string& end_id) {
    
    int entries_count = 0;
    std::string entries_data = "";
    
    auto it_stream = stream_storage_->find(key);
    if (it_stream == stream_storage_->end()) {
        return {0, ""};
    }
    
    // Start from front to back
    auto& vector_of_tuples = it_stream->second;
    for (const auto& tuple : vector_of_tuples) {
        std::string id = std::get<0>(tuple);
        std::vector<std::string> values = std::get<1>(tuple);
        
        bool include = false;
        if (end_id == "&") { // xread
            include = xaadIdIsGreaterThan(id, start_id);
        } else { // xrange
            include = (start_id == id || end_id == id || 
                        (xaadIdIsGreaterThan(id, start_id) && xaadIdIsLessThan(id, end_id)));
        }
        
        if (include) {
            std::string values_message = format_resp_array(values, true);
            entries_data += "*2\r\n$" + std::to_string(id.size()) + "\r\n" + id + "\r\n" + values_message;
            entries_count++;
        }
    }
    
    return {entries_count, entries_data};
}

// When use lambda version it captured the for checkentries with form captured values (keys was 0)
void Session::checkEntries(const asio::error_code& ec,
                  std::shared_ptr<asio::steady_timer> timer,
                  const std::vector<std::string>& keys,
                  const std::vector<std::string>& last_seen_ids,
                  int block_duration_ms,
                  bool execute) {
    if (ec) {
        std::cout << "Error or canceled, sending null" << std::endl;
        manual_write("$-1\r\n", execute);
        return;
    }

    // Check for new entries
    std::string result = "*" + std::to_string(keys.size()) + "\r\n";
    bool has_new_entries = false;
    for (size_t i = 0; i < keys.size(); i++) {
        auto [entries_count, entries_data] = getStreamEntries(keys[i], last_seen_ids[i], "&");
        result += "*2\r\n$" + std::to_string(keys[i].size()) + "\r\n" + keys[i] + "\r\n*" +
                  std::to_string(entries_count) + "\r\n" + entries_data;
        if (entries_count > 0) {
            has_new_entries = true;
        }
    }

    if (has_new_entries) {
        manual_write(result, execute);
        return;
    }

    // Handle timeout or continue polling
    if (block_duration_ms != 0 && timer->expiry() <= std::chrono::steady_clock::now()) {
        manual_write("$-1\r\n", execute);
        return;
    }

    timer->expires_after(std::chrono::milliseconds(500)); // Poll every 500ms
    timer->async_wait([this, timer, keys, last_seen_ids, block_duration_ms, execute](const asio::error_code& ec) {
        checkEntries(ec, timer, keys, last_seen_ids, block_duration_ms, execute);
    });
}

// Processes commands. Commands are sent in an array consisting of only bulk strings
void Session::processCommand(const std::string data, bool execute) {
    if (!execute) { // if executing, don't need to add it as past commands cause it already exist
        past_transactions.push_back(data);
    }
    int multi_index = -1;
    int exec_index = -1;
    for (int i = past_transactions.size() - 1; i >= 0; --i) {
        if (multi_index == -1 && (past_transactions)[i] == "*1\r\n$5\r\nMULTI\r\n") {
            multi_index = i;
        }
        
        if (exec_index == -1 && (past_transactions)[i] == "*1\r\n$4\r\nEXEC\r\n") {
            exec_index = i;
        }

        if (multi_index != -1 && exec_index != -1) {
            break;
        }
    }
    
    if (!isDataValidRedisCommand(data)) {
        std::cout << "Command not of format array of bulk strings: " << data << std::endl;
        return;
    }
    std::vector<std::string> split_data = splitString(data, '\n');
    std::vector<std::string> messages;
    bool include_size = false;
    if (multi_index != (past_transactions.size() - 1) && multi_index > exec_index && split_data[2] != "DISCARD") {
        write_simple_string("QUEUED", execute);
    } else if (split_data[2] == "ECHO") {
        // Echos back message
        messages.push_back(split_data.back());
        write(messages, include_size, execute); 
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
            write(messages, include_size, execute);  
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
        write(messages, include_size, execute);
    }
    else if (split_data[2] == "INCR") 
    {
        // INCR data from storage
        std::string key = split_data[4];

        auto it = string_storage_->find(key);
        if (it == string_storage_->end()) {
            (*string_storage_)[key] = std::make_tuple("1", TimePoint::max());
            write_integer("1", execute);
        } else {
            std::string stored_value = std::get<0>(it -> second);

            // Check if its a valid integer first
            bool is_integer = true;
            size_t start = 0;
            for (size_t i = start; i < stored_value.length(); ++i) {
                if (!std::isdigit(stored_value[i])) {
                    manual_write("-ERR value is not an integer or out of range\r\n", execute);
                    is_integer = false;
                    break;
                }
            }

            if (is_integer) {
                size_t incr_value = std::stoi(stored_value) + 1;
                TimePoint expiry_time = std::get<1>(it -> second);
                
                if (std::chrono::system_clock::now() > expiry_time) {
                    string_storage_->erase(it);
                    write_integer("1", execute);
                } else {
                    (*string_storage_)[key] = std::make_tuple(std::to_string(incr_value), expiry_time);
                    write_integer(std::to_string(incr_value), execute);
                }
            }
            
        }
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
        write(messages, include_size, execute);
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
        write(messages, include_size, execute);
    }
    else if (split_data[2] == "REPLCONF") {
        if (is_replica_ && split_data[4] == "GETACK") {
            std::string sizeStr = std::to_string(commandFromMasterSizes);
            std::string ackMsg = "*3\r\n$8\r\nREPLCONF\r\n$3\r\nACK\r\n$" + 
                     std::to_string(sizeStr.length()) + "\r\n" + 
                     sizeStr + "\r\n";
            manual_write(ackMsg, execute);
        } else {    
            if (!is_replica_ && split_data[4] == "ACK") {
                size_t acknowledgedBytes = std::stoull(split_data[6]);
                this->lastAcknowledgedBytes = acknowledgedBytes;
            } else {
                // Second Part of handshake with replicas
                messages.push_back("OK");
                write(messages, include_size, execute);  
            }
        }
    }
    else if (split_data[2] == "PSYNC") {
        if (!is_replica_) {
            // Third Part of handshake with replicas
            std::string message = "+FULLRESYNC " + master_repl_id_ + " " + std::to_string(master_repl_offset_);
            messages.push_back(message);
            write(messages, include_size, execute);
            g_replica_sessions.push_back(shared_from_this()); // new replica connected
            std::string empty_rdb = "\x52\x45\x44\x49\x53\x30\x30\x31\x31\xfa\x09\x72\x65\x64\x69\x73\x2d\x76\x65\x72\x05\x37\x2e\x32\x2e\x30\xfa\x0a\x72\x65\x64\x69\x73\x2d\x62\x69\x74\x73\xc0\x40\xfa\x05\x63\x74\x69\x6d\x65\xc2\x6d\x08\xbc\x65\xfa\x08\x75\x73\x65\x64\x2d\x6d\x65\x6d\xc2\xb0\xc4\x10\x00\xfa\x08\x61\x6f\x66\x2d\x62\x61\x73\x65\xc0\x00\xff\xf0\x6e\x3b\xfe\xc0\xff\x5a\xa2";
            std::string return_msg = "$" + std::to_string(empty_rdb.length()) + "\r\n" + empty_rdb;
            manual_write(return_msg, execute);
        }
    }
    else if (split_data[2] == "WAIT") {
        int numReplicas = std::stoi(split_data[4]);
        int timeoutMs = std::stoi(split_data[6]);
        if (numReplicas == 0 || timeoutMs == 0) {
            manual_write(":0\r\n", execute);
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

        checkAcks = [this, self, timer, numReplicas, &checkAcks, offset, execute](const asio::error_code& ec) {
            if (ec) {
                // Timer was canceled or error occurred
                manual_write(":0\r\n", execute);
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
                manual_write(":" + std::to_string(replicasAcknowledged) + "\r\n", execute);
                return;
            }
                
            // Otherwise, schedule another check after a short delay
            // timer->expires_after(std::chrono::milliseconds(50)); // This is less likely to be triggered since it starts after waiting for expiry_After
            // timer->async_wait(checkAcks);
        };
        
        // Start the acknowledgment checking process
        timer->async_wait(checkAcks);            // starts after waiting for expiry_After
    }
    else if (split_data[2] == "TYPE") {
        // Get type of data from storage
        std::string key = split_data[4]; // TODO: I think by right a key can only hold one type of data at a time

        auto it_stream = stream_storage_->find(key);
        if (it_stream != stream_storage_->end()) {
            write_simple_string("stream", execute);
        } else {
            auto it_string = string_storage_->find(key);
            if (it_string == string_storage_->end()) {
                write_simple_string("none", execute);
            } else {
                std::string stored_value = std::get<0>(it_string -> second);
                TimePoint expiry_time = std::get<1>(it_string -> second);
                
                if (std::chrono::system_clock::now() > expiry_time) {
                    string_storage_->erase(it_string);
                    write_simple_string("none", execute);
                } else {
                    write_simple_string("string", execute); 
                }
            }   
        }
    }
    else if (split_data[2] == "XADD" || split_data[2] == "xadd") {
        std::string key = split_data[4];
        std::string id = split_data[6];
        std::string leftPart_Id, rightPart_Id;
        std::vector<std::string> values;
        // Start at index 7 (after command, key, ID and their length prefixes)
        for (size_t i = 7; i < split_data.size(); i += 2) {
            // Skip the length indicator ($3) and include only the actual value
            if (i + 1 < split_data.size()) {
                values.push_back(split_data[i + 1]);
            }
        }
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
            new_entry.push_back(std::make_tuple(id, values));
            (*stream_storage_)[key] = new_entry;
            std::cout << "added as new entry with key" << key << std::endl;
            write_bulk_string(id, execute);
        } else {
            // check if entry is valid
            const auto& last_tuple = it_stream->second.back();
            std::string id_last_entry = std::get<0>(last_tuple);
            if (!xaadIdIsGreaterThan(id, "0-0")) {
                manual_write("-ERR The ID specified in XADD must be greater than 0-0\r\n", execute);
            } else if (!xaadIdIsGreaterThan(id, id_last_entry)) {
                manual_write("-ERR The ID specified in XADD is equal or smaller than the target stream top item\r\n", execute);
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
                vector_of_tuples.push_back(std::make_tuple(id, values));
                write_bulk_string(id, execute);
            }
        }
    }
    else if (split_data[2] == "XRANGE" || split_data[2] == "xrange") {
        std::string key = split_data[4];
        // get start
        std::string start_id = split_data[6];
        size_t dashPos_start_id = start_id.find('-');
        if (start_id == "-") {
            start_id = "0-1"; // give the smallest possible id so that it will be the beginning of the stream
        } else if (dashPos_start_id == std::string::npos) {
            start_id = start_id + "-0";
        }
        // get end
        std::string end_id = split_data[8];
        size_t dashPos_end_id = end_id.find('-');
        if (end_id == "+") {
            end_id = "+";
        } else if (dashPos_end_id == std::string::npos) {
            end_id = end_id + "-0";
        }

        auto [entries_count, entries_data] = getStreamEntries(key, start_id, end_id);
        manual_write("*" + std::to_string(entries_count) + "\r\n" + entries_data, execute);
    }
    else if (split_data[2] == "XREAD" || split_data[2] == "xread") {
        // Find the "block" keyword
        size_t block_index = 0;
        int block_duration_ms = 0;
        for (size_t i = 0; i < split_data.size(); i++) {
            if (split_data[i] == "block") {
                block_index = i;
                block_duration_ms = std::stoi(split_data[i + 2]);
                break;
            }
        }
        // Find the "STREAMS" keyword
        size_t streams_index = 0;
        for (size_t i = 3; i < split_data.size(); i++) {
            if (split_data[i] == "streams") {
                streams_index = i;
                break;
            }
        }
        
        if (streams_index == 0) {
            manual_write("-ERR Syntax error\r\n", execute);
            return;
        }
        
        // Extract the keys and IDs
        std::vector<std::string> keys;
        std::vector<std::string> ids;
        
        // Calculate where keys and IDs begin (since we know its half keys half ids)
        size_t total_items = split_data.size() - streams_index - 1;
        size_t num_keys = total_items / 2;
        
        // Extract keys (first half after "STREAMS")
        for (size_t i = 1; i < num_keys; i+=2) {
            keys.push_back(split_data[streams_index + 1 + i]);
        }
        
        // Extract IDs (second half after keys)
        for (size_t i = 1; i < num_keys; i+=2) {
            ids.push_back(split_data[streams_index + 1 + num_keys + i]);
        }
        if (block_index == 0) {
            // Non-blocking XREAD: respond immediately with existing entries
            std::string result = "*" + std::to_string(keys.size()) + "\r\n";
            for (size_t i = 0; i < keys.size(); i++) {
                auto [entries_count, entries_data] = getStreamEntries(keys[i], ids[i], "&");
                result += "*2\r\n$" + std::to_string(keys[i].size()) + "\r\n" + keys[i] + "\r\n*" +
                          std::to_string(entries_count) + "\r\n" + entries_data;
            }
            manual_write(result, execute);
        } else {
            // Blocking XREAD
            auto self = shared_from_this();
            auto timer = std::make_shared<asio::steady_timer>(socket_.get_executor());
            
            if (block_duration_ms == 0) {
                timer->expires_after(std::chrono::milliseconds(50)); // Initial polling interval for blocking
            } else {
                timer->expires_after(std::chrono::milliseconds(block_duration_ms)); // Total timeout
            }

            // Track the highest existing ID for each key as the baseline
            std::vector<std::string> last_seen_ids(keys.size());
            for (size_t i = 0; i < keys.size(); i++) {
                if (ids[i] == "$") { // Not too sure why becauase I thought all blocking is to block till new entries. Currently if lets say we have 3 existing entries 1, 2, 3 and the stream id is 1, the last seen ids will take it as 3 so only checks for 4 and above
                    // Use the current maximum ID if $ is specified
                    auto it = stream_storage_->find(keys[i]);
                    if (it != stream_storage_->end() && !it->second.empty()) {
                        last_seen_ids[i] = std::get<0>(it->second.back());
                    } else {
                        last_seen_ids[i] = "0-0"; // Default minimal ID for empty streams
                    }
                } else {
                    // Use the provided ID and update if necessary
                    auto it = stream_storage_->find(keys[i]);
                    if (it != stream_storage_->end() && !it->second.empty()) {
                        last_seen_ids[i] = std::get<0>(it->second.back());
                        if (xaadIdIsGreaterThan(ids[i], last_seen_ids[i])) { 
                            last_seen_ids[i] = ids[i];
                        }
                    } else {
                        last_seen_ids[i] = ids[i];
                    }
                }
            }

            // Start with an immediate async check
            timer->async_wait([this, self, timer, keys, last_seen_ids, block_duration_ms, execute](const asio::error_code& ec) {
                checkEntries(ec, timer, keys, last_seen_ids, block_duration_ms, execute);
            });
        }
    }
    else if (split_data[2] == "MULTI") {
        write_simple_string("OK", execute);
    }
    else if (split_data[2] == "EXEC") {
        if (past_transactions.size() >= 2) { // at least have two commands
            if (exec_index - multi_index == 1) {
                past_transactions.clear();
                manual_write("*0\r\n", execute);
            } else {
                for (int i = multi_index + 1; i < exec_index; ++i) {
                    std::cout << "command sent to executed: " << past_transactions[i] << i << exec_index << std::endl;
                    std::cout << past_transactions[0] << std::endl;
                    std::cout << past_transactions[1] << std::endl;
                    std::cout << past_transactions[2] << std::endl;
                    processCommand(past_transactions[i], true);
                }
                std::string response = "*" + std::to_string(exec_responses.size()) + "\r\n";
                for (const auto& resp : exec_responses) {
                    response += resp;
                }
                exec_responses.clear();
                past_transactions.clear(); 
                manual_write(response, false);
            }
        } else {
            manual_write("-ERR EXEC without MULTI\r\n", execute);
        }
    }
    else if (split_data[2] == "DISCARD") {
        if (multi_index == -1) {
            manual_write("-ERR DISCARD without MULTI\r\n");
        } else {
            past_transactions.clear();
            write_simple_string("OK", execute);
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

// TODO: Wrap stuff with this function
std::string Session::format_resp_array(std::vector<std::string> messages, bool formatContent) {
    std::stringstream msg_stream;
    msg_stream << "*" << messages.size() << "\r\n";
    if (formatContent) {
        for (std::string message: messages) {
            msg_stream << "$" << message.size() << "\r\n" << message << "\r\n";
        }
    }

    return msg_stream.str();
}

// Write without any parsing
void Session::manual_write(std::string message, bool execute) {
    auto self(shared_from_this());
    std::cout << "MESSAGE SENT (manual)..: " << message << std::endl;
    if (execute) {
        exec_responses.push_back(message);
    } else {
        asio::async_write(socket_, asio::buffer(message, message.size()),
            [this, self](asio::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    read();  // Continue reading after successful write
                } else {
                    std::cerr << "Write error: " << ec.message() << std::endl;
                }
            });
    }
}

void Session::write_simple_string(std::string message, bool execute) {
    auto self(shared_from_this());
    std::string formatted_message = "+" + message + "\r\n";
    std::cout << "MESSAGE SENT (simple string)..: " << formatted_message << std::endl;
    if (execute) {
        exec_responses.push_back(formatted_message);
    } else {
        asio::async_write(socket_, asio::buffer(formatted_message, formatted_message.size()),
            [this, self](asio::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    read();  // Continue reading after successful write
                } else {
                    std::cerr << "Write error: " << ec.message() << std::endl;
                }
            });

    }
}

void Session::write_integer(std::string message, bool execute) {
    auto self(shared_from_this());
    std::string formatted_message = ":" + message + "\r\n";
    std::cout << "MESSAGE SENT (integer)..: " << formatted_message << std::endl;
    if (execute) {
        exec_responses.push_back(formatted_message);
    } else {
        asio::async_write(socket_, asio::buffer(formatted_message, formatted_message.size()),
            [this, self](asio::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    read();  // Continue reading after successful write
                } else {
                    std::cerr << "Write error: " << ec.message() << std::endl;
                }
            });
    }
}

void Session::write_bulk_string(std::string message, bool execute) {
    auto self(shared_from_this());
    std::string formatted_message = "$" + std::to_string(message.size()) + "\r\n" + message + "\r\n";
    std::cout << "MESSAGE SENT (bulk string)..: " << formatted_message << std::endl;
    if (execute) {
        exec_responses.push_back(formatted_message);
    } else {
        asio::async_write(socket_, asio::buffer(formatted_message, formatted_message.size()),
            [this, self](asio::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    read();  // Continue reading after successful write
                } else {
                    std::cerr << "Write error: " << ec.message() << std::endl;
                }
            });
    }
}

// Write with formatting, adding size of all messages and size of individual message
void Session::write(std::vector<std::string> messages, bool size, bool execute) {
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
    if (execute) {
        exec_responses.push_back(msg);
    } else {
        asio::async_write(socket_, asio::buffer(msg, msg.size()),
            [this, self](asio::error_code ec, std::size_t /*length*/) {
                if (!ec) {
                    read();  // Continue reading after successful write
                } else {
                    std::cerr << "Write error: " << ec.message() << std::endl;
                }
            });
    }
}

}
