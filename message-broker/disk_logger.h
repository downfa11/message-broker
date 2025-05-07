#ifndef DISK_LOGGER_H
#define DISK_LOGGER_H

#include <windows.h>
#include <string>
#include <mutex>
#include <iostream>
#include <vector>
#include <atomic>
#include <thread>
#include <fstream>
#include <format>
#include <string_view>

#include "log_cursor.h"

class DiskLogger {
public:
    DiskLogger(std::string baseFilename, size_t segmentSize)
        : baseName(std::move(baseFilename)),
        segmentSize(segmentSize),
        currentOffset(0),
        currentSegmentIndex(0),
        stopFlush(false) {
        load_offset();
        open_new_segment();
        flushThread = std::jthread([this] { flush_loop(); });  // C++20 std::jthread
    }

    DiskLogger(const DiskLogger&) = delete;
    DiskLogger& operator=(const DiskLogger&) = delete;
    DiskLogger(DiskLogger&&) noexcept = default;
    DiskLogger& operator=(DiskLogger&&) noexcept = default;

    ~DiskLogger() {
        stopFlush = true;
        if (flushThread.joinable()) flushThread.join();
        flush();
        close_handles();
        save_offset();
    }

    void log(std::string_view message) {
        std::lock_guard lock(mtx);
        size_t len = message.size();

        if (currentOffset + len + 1 >= segmentSize) {
            rotate_segment();
        }

        std::memcpy(static_cast<char*>(mapView) + currentOffset, message.data(), len);
        static_cast<char*>(mapView)[currentOffset + len] = '\n';
        currentOffset += len + 1;
    }
    std::optional<std::string> read_next(LogCursor& cursor) {
        std::lock_guard<std::mutex> lock(mtx);

        if (cursor.segmentIndex > currentSegmentIndex) return std::nullopt;

        HANDLE h = open_segment(cursor.segmentIndex);
        void* view = map_view(h);

        const char* data = reinterpret_cast<const char*>(view) + cursor.offset;
        size_t i = cursor.offset;

        while (i < segmentSize) {
            if (data[i] == '\n') {
                std::string msg(data + cursor.offset, i - cursor.offset);
                cursor.offset = i + 1;

                if (cursor.offset >= segmentSize) {
                    cursor.segmentIndex++;
                    cursor.offset = 0;
                }

                return msg;
            }
            ++i;
        }

        return std::nullopt;
    }
    std::vector<std::string> read_all(size_t segmentIndex) {
        std::vector<std::string> lines;
        const auto filename = get_segment_filename(segmentIndex);

        HANDLE h = CreateFileA(filename.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
        if (h == INVALID_HANDLE_VALUE) {
            std::cerr << std::format("[DiskLogger] Cannot open file: {}\n", filename);
            return lines;
        }

        DWORD fSize = GetFileSize(h, nullptr);
        HANDLE hMap = CreateFileMappingA(h, nullptr, PAGE_READONLY, 0, 0, nullptr);
        if (!hMap) {
            CloseHandle(h);
            return lines;
        }

        void* view = MapViewOfFile(hMap, FILE_MAP_READ, 0, 0, 0);
        if (!view) {
            CloseHandle(hMap);
            CloseHandle(h);
            return lines;
        }

        const char* data = static_cast<const char*>(view);
        size_t start = 0;
        for (size_t i = 0; i < fSize; ++i) {
            if (data[i] == '\n') {
                lines.emplace_back(data + start, i - start);
                start = i + 1;
            }
        }

        UnmapViewOfFile(view);
        CloseHandle(hMap);
        CloseHandle(h);
        return lines;
    }

private:
    std::mutex mtx;
    std::string baseName;
    size_t segmentSize;
    size_t currentOffset;
    size_t currentSegmentIndex;

    HANDLE hFile = INVALID_HANDLE_VALUE;
    HANDLE hMapping = nullptr;
    void* mapView = nullptr;

    std::jthread flushThread;
    std::atomic<bool> stopFlush;

    void rotate_segment() {
        flush();
        close_handles();
        ++currentSegmentIndex;
        currentOffset = 0;
        open_new_segment();
        save_offset();
    }

    void close_handles() {
        if (mapView) {
            UnmapViewOfFile(mapView);
            mapView = nullptr;
        }
        if (hMapping) {
            CloseHandle(hMapping);
            hMapping = nullptr;
        }
        if (hFile != INVALID_HANDLE_VALUE) {
            CloseHandle(hFile);
            hFile = INVALID_HANDLE_VALUE;
        }
    }

    void open_new_segment() {
        const auto filename = get_segment_filename(currentSegmentIndex);

        hFile = CreateFileA(filename.c_str(), GENERIC_READ | GENERIC_WRITE, 0, nullptr, OPEN_ALWAYS,
            FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN, nullptr);
        if (hFile == INVALID_HANDLE_VALUE) {
            std::cerr << std::format("[DiskLogger] Cannot open segment file: {}\n", filename);
            return;
        }

        hMapping = CreateFileMappingA(hFile, nullptr, PAGE_READWRITE, 0, static_cast<DWORD>(segmentSize), nullptr);
        if (!hMapping) {
            std::cerr << "[DiskLogger] hMapping failed\n";
            return;
        }

        mapView = MapViewOfFile(hMapping, FILE_MAP_WRITE, 0, 0, segmentSize);
        if (!mapView) {
            std::cerr << "[DiskLogger] mapView failed\n";
        }
    }

    std::string get_segment_filename(size_t index) const {
        return std::format("{}{}.seg{}", baseName, "", index);
    }

    void flush_loop() {
        while (!stopFlush) {
            std::this_thread::sleep_for(std::chrono::seconds(2));
            flush();
        }
    }

    void flush() {
        std::lock_guard lock(mtx);
        if (mapView) {
            FlushViewOfFile(mapView, currentOffset);
            FlushFileBuffers(hFile);
        }
    }

    void save_offset() const {
        std::ofstream ofs(baseName + ".offset", std::ios::binary);
        ofs.write(reinterpret_cast<const char*>(&currentSegmentIndex), sizeof(currentSegmentIndex));
        ofs.write(reinterpret_cast<const char*>(&currentOffset), sizeof(currentOffset));
    }

    void load_offset() {
        std::ifstream ifs(baseName + ".offset", std::ios::binary);
        if (ifs) {
            ifs.read(reinterpret_cast<char*>(&currentSegmentIndex), sizeof(currentSegmentIndex));
            ifs.read(reinterpret_cast<char*>(&currentOffset), sizeof(currentOffset));
        }
    }

    HANDLE open_segment(size_t index) {
        std::string filename = get_segment_filename(index);
        HANDLE h = CreateFileA(filename.c_str(), GENERIC_READ, FILE_SHARE_READ, NULL, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, NULL);
        if (h == INVALID_HANDLE_VALUE) {
            std::cerr << "[DiskLogger] Cannot open file: " << filename << std::endl;
        }
        return h;
    }

    void* map_view(HANDLE h) {
        HANDLE hMap = CreateFileMappingA(h, NULL, PAGE_READONLY, 0, 0, NULL);
        if (!hMap) {
            std::cerr << "[DiskLogger] File mapping failed" << std::endl;
            CloseHandle(h);
            return nullptr;
        }

        void* view = MapViewOfFile(hMap, FILE_MAP_READ, 0, 0, 0);
        if (!view) {
            std::cerr << "[DiskLogger] MapViewOfFile failed" << std::endl;
            CloseHandle(hMap);
            CloseHandle(h);
        }
        return view;
    }
};

#endif
