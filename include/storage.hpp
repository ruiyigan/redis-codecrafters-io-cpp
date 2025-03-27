#ifndef STORAGE_HPP
#define STORAGE_HPP

#include <chrono>
#include <unordered_map>
#include <string>
#include <tuple>
#include <vector>

namespace redis_server {

// Type aliases
using TimePoint = std::chrono::system_clock::time_point;
using StringStorageType = std::unordered_map<std::string, std::tuple<std::string, TimePoint>>;
using StreamStorageType = std::unordered_map<std::string, std::vector<std::tuple<std::string, std::vector<std::string>>>>;

} // namespace redis_server

#endif // STORAGE_HPP
