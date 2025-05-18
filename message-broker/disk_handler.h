#pragma once

#include <windows.h>
#include <string>
#include <mutex>
#include <vector>
#include <optional>
#include <atomic>
#include <thread>
#include <string_view>

struct LogCursor {
    size_t segmentIndex;
    size_t offset;
};

class DiskHandler {
public:
    DiskHandler(std::string baseFilename, size_t segmentSize);
    ~DiskHandler();

    DiskHandler(const DiskHandler&) = delete;
    DiskHandler& operator=(const DiskHandler&) = delete;
    DiskHandler(DiskHandler&&) noexcept = default;
    DiskHandler& operator=(DiskHandler&&) noexcept = default;

    void log(std::string_view message);
    std::optional<std::string> read_next(LogCursor& cursor);
    std::vector<std::string> read_all(size_t segmentIndex);

private:
    std::mutex mtx;
    std::string baseName;
    size_t segmentSize;
    size_t currentOffset;
    size_t currentSegmentIndex;

    HANDLE hFile = INVALID_HANDLE_VALUE;
    HANDLE hMap= nullptr;

    void* mapView = nullptr;

    std::jthread flushThread;
    std::atomic<bool> stopFlush;

    void rotate_segment();
    void close_handles();
    void open_new_segment();
    std::string get_segment_filename(size_t index) const;
    void flush_loop();
    void flush();
    void save_offset() const;
    void load_offset();
    HANDLE open_segment(size_t index);
    void* map_view(HANDLE h);
};