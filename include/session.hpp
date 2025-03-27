#ifndef SESSION_HPP
#define SESSION_HPP

#include <memory>
#include <vector>
#include <asio.hpp>
#include "storage.hpp"

using asio::ip::tcp; 
namespace redis_server {

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(
        asio::ip::tcp::socket socket, 
        std::shared_ptr<StringStorageType> storage,
        std::string dir,
        std::string dbfilename,
        std::string masterdetails,
        std::string master_repl_id,
        unsigned master_repl_offset,
        std::shared_ptr<StreamStorageType> stream_storage
    );
    
    void start();
    void setReplica(bool replica);

    inline static std::vector<std::shared_ptr<Session>> g_replica_sessions;

private:
    // Private methods (declarations only)
    void read();
    void propagate(const std::string& command);
    void processCommands(std::string data);
    void processCommand(const std::string data, bool execute = false);
    std::vector<std::string> splitString(const std::string& input, char delimiter);
    std::vector<std::string> splitMultipleArrayCommands(const std::string& input);
    void getValidDataTypeChunks(std::string data, std::vector<std::string>& validCommands);
    void processDataByType(std::string command);
    bool isDataValidRedisCommand(const std::string& data);
    bool hasAcknowledged(size_t expectedOffset);
    bool xaadIdIsGreaterThan(const std::string& newId, const std::string& oldId);
    bool xaadIdIsLessThan(const std::string& newId, const std::string& oldId);
    std::pair<int, std::string> getStreamEntries(const std::string& key, const std::string& start_id, const std::string& end_id);
    void checkEntries(const asio::error_code& ec, std::shared_ptr<asio::steady_timer> timer,
                     const std::vector<std::string>& keys, const std::vector<std::string>& last_seen_ids,
                     int block_duration_ms, bool execute);
    std::string format_resp_array(std::vector<std::string> messages, bool formatContent = false);
    void manual_write(std::string message, bool execute = false);
    void write_simple_string(std::string message, bool execute = false);
    void write_integer(std::string message, bool execute = false);
    void write_bulk_string(std::string message, bool execute = false);
    void write(std::vector<std::string> messages, bool size = false, bool execute = false);

    // Attributes
    asio::ip::tcp::socket socket_;
    std::array<char, 1024> buffer_;
    std::shared_ptr<StringStorageType> string_storage_;
    std::shared_ptr<StreamStorageType> stream_storage_;
    std::vector<std::string> past_transactions;
    std::vector<std::string> exec_responses;
    std::string dir_;
    std::string dbfilename_;
    std::string masterdetails_;
    std::string master_repl_id_;
    unsigned master_repl_offset_;
    bool is_replica_ = false;
    size_t commandFromMasterSizes = 0;
    size_t propagatedCommandSizes =  0;
    size_t lastAcknowledgedBytes = 0;
    std::shared_ptr<TimePoint> blocking_time;
    std::shared_ptr<bool> isBlocked = std::make_shared<bool>(false);
};

} // namespace redis_server

#endif // SESSION_HPP
