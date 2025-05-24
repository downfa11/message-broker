#include "disk_handler.h"

#include <fstream>
#include <iostream>
#include <chrono>
#include <format>

DiskHandler::DiskHandler(std::string baseFilename, size_t segmentSize)
    : baseName(std::move(baseFilename)),
    segmentSize(segmentSize),
    currentOffset(0),
    currentSegmentIndex(0),
    stopFlush(false) {
        load_offset();
        open_new_segment();
        flushThread = std::jthread([this] { flush_loop(); 
    });
}

DiskHandler::~DiskHandler() {
    stopFlush = true;

    flush();
    close_handles();
    save_offset();
}


void DiskHandler::log(std::string_view level, std::string_view message) {
    std::lock_guard lock(mtx);
   
    std::string timestamp = convert_timestamp();
    std::string formatted = std::format("[{}] timestamp: {}, message: {}\n", level, timestamp, message);
    size_t len = formatted.size();

    if (len >= segmentSize) {
        std::cerr << "[disk error] log too large to fit in segment" << std::endl;
        return;
    }

    if (currentOffset + len >= segmentSize) {
        if (!rotate_segment()) {
            std::cerr << "[disk error] rotate_segment failed" << std::endl;
            return;
        }
    }

    std::memcpy(static_cast<char*>(mapView) + currentOffset, formatted.data(), len);
    currentOffset += len;
}

std::optional<std::string> DiskHandler::read_next(LogCursor& cursor) {
    std::lock_guard<std::mutex> lock(mtx);

    if (cursor.segmentIndex > currentSegmentIndex) 
        return std::nullopt;

    HANDLE hFile = open_segment(cursor.segmentIndex);

    if (hFile == INVALID_HANDLE_VALUE) 
        return std::nullopt;

    HANDLE hMap = CreateFileMappingA(hFile, nullptr, PAGE_READONLY, 0, 0, nullptr);

    if (!hMap) {
        CloseHandle(hFile);
        return std::nullopt;
    }

    void* view = MapViewOfFile(hMap, FILE_MAP_READ, 0, 0, 0);

    if (!view) {
        CloseHandle(hMap);
        CloseHandle(hFile);
        return std::nullopt;
    }

    const char* data = static_cast<const char*>(view);
    size_t i = cursor.offset;

    while (i < segmentSize) {
        if (data[i] == '\n') {
            std::string msg(data + cursor.offset, i - cursor.offset);
            cursor.offset = i + 1;

            if (cursor.offset >= segmentSize) {
                cursor.segmentIndex++;
                cursor.offset = 0;
            }

            UnmapViewOfFile(view);
            CloseHandle(hMap);
            CloseHandle(hFile);
            return msg;
        }
        ++i;
    }

    UnmapViewOfFile(view);
    CloseHandle(hMap);
    CloseHandle(hFile);

    return std::nullopt;
}

std::vector<std::string> DiskHandler::read_all(size_t segmentIndex) {
    std::vector<std::string> lines;
    std::string filename = get_segment_filename(segmentIndex);

    HANDLE hFile = open_segment(segmentIndex);
    if (hFile == INVALID_HANDLE_VALUE) 
        return lines;

    HANDLE hMap = CreateFileMappingA(hFile, nullptr, PAGE_READONLY, 0, 0, nullptr);
    if (!hMap) {
        CloseHandle(hFile);
        return lines;
    }

    void* view = MapViewOfFile(hMap, FILE_MAP_READ, 0, 0, 0);
    if (!view) {
        CloseHandle(hMap);
        CloseHandle(hFile);
        return lines;
    }

    const char* data = static_cast<const char*>(view);
    size_t start = 0;

    for (size_t i = 0; i < segmentSize; ++i) {
        if (data[i] == '\n') {
            lines.emplace_back(data + start, i - start);
            start = i + 1;
        }
    }

    UnmapViewOfFile(view);
    CloseHandle(hMap);
    CloseHandle(hFile);
    return lines;
}

void DiskHandler::flush_loop() {
    while (!stopFlush) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        flush();
    }
}

void DiskHandler::flush() {
    std::lock_guard lock(mtx);
    if (mapView) {
        FlushViewOfFile(mapView, 0);
    }
}

bool DiskHandler::rotate_segment() {
    flush();
    close_handles();
    currentSegmentIndex++;
    currentOffset = 0;
    return open_new_segment();
}

bool DiskHandler::open_new_segment() {
    std::string filename = get_segment_filename(currentSegmentIndex);

    hFile = CreateFileA(filename.c_str(), GENERIC_WRITE | GENERIC_READ,
        0, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (hFile == INVALID_HANDLE_VALUE) {
        std::cerr << "[disk error] Failed to open file: " << filename << std::endl;
        return false;
    }

    if (SetFilePointer(hFile, static_cast<LONG>(segmentSize), nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
        std::cerr << "[disk error] Failed to set file pointer\n";
        CloseHandle(hFile);
        hFile = INVALID_HANDLE_VALUE;
        return false;
    }

    if (!SetEndOfFile(hFile)) {
        std::cerr << "[disk error] Failed to set end of file\n";
        CloseHandle(hFile);
        hFile = INVALID_HANDLE_VALUE;
        return false;
    }

    hMap = CreateFileMappingA(hFile, nullptr, PAGE_READWRITE, 0, static_cast<DWORD>(segmentSize), nullptr);
    if (hMap == nullptr) {
        std::cerr << "[disk error] Failed to create file mapping\n";
        CloseHandle(hFile);
        hFile = INVALID_HANDLE_VALUE;
        return false;
    }

    mapView = MapViewOfFile(hMap, FILE_MAP_ALL_ACCESS, 0, 0, segmentSize);
    if (mapView == nullptr) {
        std::cerr << "[disk error] Failed to map view of file\n";
        CloseHandle(hMap);
        CloseHandle(hFile);
        hMap = nullptr;
        hFile = INVALID_HANDLE_VALUE;
        return false;
    }

    return true;
}

void DiskHandler::close_handles() {
    if (mapView) {
        UnmapViewOfFile(mapView);
        mapView = nullptr;
    }
    if (hMap) {
        CloseHandle(hMap);
        hMap = nullptr;
    }
    if (hFile != INVALID_HANDLE_VALUE && hFile != nullptr) {
        CloseHandle(hFile);
        hFile = INVALID_HANDLE_VALUE;
    }
}


HANDLE DiskHandler::open_segment(size_t index) {
    std::string filename = get_segment_filename(index);
    return CreateFileA(filename.c_str(), GENERIC_READ, FILE_SHARE_READ, nullptr,
        OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, nullptr);
}

std::string DiskHandler::get_segment_filename(size_t index) const {
    return std::format("{}_{:05}.log", baseName, index);
}

void DiskHandler::load_offset() {
    std::ifstream file(baseName + ".meta");
    if (file) {
        file >> currentSegmentIndex >> currentOffset;
    }
}

void DiskHandler::save_offset() const {
    std::ofstream file(baseName + ".meta", std::ios::trunc);
    file << currentSegmentIndex << ' ' << currentOffset;
}

std::string DiskHandler::convert_timestamp() {
    auto now = std::chrono::system_clock::now();
    std::time_t now_c = std::chrono::system_clock::to_time_t(now);
    std::tm local_tm;

    localtime_s(&local_tm, &now_c);

    char time_buf[32];
    std::strftime(time_buf, sizeof(time_buf), "%Y-%m-%d %H:%M:%S", &local_tm);

    return std::string(time_buf);
}