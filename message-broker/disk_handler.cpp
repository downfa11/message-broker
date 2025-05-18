#include "disk_handler.h"

#include <fstream>
#include <iostream>
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

    if (flushThread.joinable()) {
        flushThread.join();
    }

    flush();
    close_handles();
    save_offset();
}

void DiskHandler::log(std::string_view message) {
    std::lock_guard lock(mtx);
    size_t len = message.size();

    if (currentOffset + len + 1 >= segmentSize) {
        rotate_segment();
    }

    std::memcpy(static_cast<char*>(mapView) + currentOffset, message.data(), len);
    reinterpret_cast<char*>(mapView)[currentOffset + len] = '\n';
    currentOffset += len + 1;
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

void DiskHandler::rotate_segment() {
    flush();
    close_handles();
    currentSegmentIndex++;
    currentOffset = 0;
    open_new_segment();
}

void DiskHandler::open_new_segment() {
    std::string filename = get_segment_filename(currentSegmentIndex);

    hFile = CreateFileA(filename.c_str(), GENERIC_WRITE | GENERIC_READ,
        0, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);
    if (hFile == INVALID_HANDLE_VALUE) {
        std::cerr << "[disk error] Failed to open file: " << filename << std::endl;
        return;
    }

    if (SetFilePointer(hFile, static_cast<LONG>(segmentSize), nullptr, FILE_BEGIN) == INVALID_SET_FILE_POINTER) {
        std::cerr << "[disk error] Failed to set file pointer\n";
        CloseHandle(hFile);
        hFile = INVALID_HANDLE_VALUE;
        return;
    }

    if (!SetEndOfFile(hFile)) {
        std::cerr << "[disk error] Failed to set end of file\n";
        CloseHandle(hFile);
        hFile = INVALID_HANDLE_VALUE;
        return;
    }

    hMap = CreateFileMappingA(hFile, nullptr, PAGE_READWRITE, 0, static_cast<DWORD>(segmentSize), nullptr);
    if (hMap == nullptr) {
        std::cerr << "[disk error] Failed to create file mapping\n";
        CloseHandle(hFile);
        hFile = INVALID_HANDLE_VALUE;
        return;
    }

    mapView = MapViewOfFile(hMap, FILE_MAP_ALL_ACCESS, 0, 0, segmentSize);
    if (mapView == nullptr) {
        std::cerr << "[disk error] Failed to map view of file\n";
        CloseHandle(hMap);
        CloseHandle(hFile);
        hMap = nullptr;
        hFile = INVALID_HANDLE_VALUE;
        return;
    }
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
