#include "disk_handler.h"

#include <fstream>
#include <iostream>
#include <chrono>
#include <format>
#include <filesystem>

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
        std::cerr << "[debug] rotate_segment, currentOffset=" << currentOffset << ", length=" << len << ", segmentSize=" << segmentSize << std::endl;

        if (!rotate_segment()) {
            std::cerr << "[disk error] rotate_segment failed" << std::endl;
            return;
        }
    }

    // std::cout << "[disk log] log(" << formatted.data();
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
    try {
        if (mapView) {
            if (!FlushViewOfFile(mapView, 0)) {
                std::cerr << "[disk error] FlushViewOfFile failed: " << GetLastError() << std::endl;
            }
        }
    }
    catch (const std::system_error& e) {
        std::cerr << "[disk error] std::system_error in flush(): " << e.what() << std::endl;
    }
    catch (...) {
        std::cerr << "[disk error] exception in flush()" << std::endl;
    }
}

bool DiskHandler::rotate_segment() {
    flush();
    close_handles();
    currentSegmentIndex++;
    currentOffset = 0;

    std::cerr << "[debug] rotate_segment: new index = " << currentSegmentIndex << ", offset reset to 0"<< std::endl;

    if (!open_new_segment()) {
        std::cerr << "[disk error] open_new_segment error. Reverting to index 0." << std::endl;
        currentSegmentIndex = 0;
        currentOffset = 0;
        if (!open_new_segment()) {
            std::cerr << "[disk error] init offset, segment. open_new_segment error again." << std::endl;
            return false;
        }
    }

    save_offset();
    return true;
}

bool DiskHandler::open_new_segment() {
    std::cout << "open_new_segment " << currentSegmentIndex << std::endl;
    std::string filename = get_segment_filename(currentSegmentIndex);

    hFile = CreateFileA(filename.c_str(), GENERIC_WRITE | GENERIC_READ,
        0, nullptr, OPEN_ALWAYS, FILE_ATTRIBUTE_NORMAL, nullptr);

    if (hFile == INVALID_HANDLE_VALUE) {
        std::cerr << "[disk error] INVALID_HANDLE_VALUE: " << filename << std::endl;
        return false;
    }

    LARGE_INTEGER li;
    li.QuadPart = static_cast<LONGLONG>(segmentSize);
    if (!SetFilePointerEx(hFile, li, nullptr, FILE_BEGIN) || !SetEndOfFile(hFile)) {
        std::cerr << "[disk error] Failed to set file size" << std::endl;
        CloseHandle(hFile);
        hFile = INVALID_HANDLE_VALUE;
        return false;
    }

    hMap = CreateFileMappingA(hFile, nullptr, PAGE_READWRITE, 0, segmentSize, nullptr);
    if (!hMap) {
        std::cerr << "[disk error] CreateFileMappingA error" << std::endl;
        CloseHandle(hFile);
        hFile = INVALID_HANDLE_VALUE;
        return false;
    }

    mapView = MapViewOfFile(hMap, FILE_MAP_ALL_ACCESS, 0, 0, segmentSize);
    if (!mapView) {
        std::cerr << "[disk error] MapViewOfFile error" << std::endl;
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
    if (!file) {
        std::cerr << "[debug] No meta file found" << std::endl;
        currentSegmentIndex = 0;
        currentOffset = 0;
        return;
    }

    if (!(file >> currentSegmentIndex >> currentOffset)) {
        std::cerr << "[debug] Failed to read meta file, initing offset." << std::endl;
        currentSegmentIndex = 0;
        currentOffset = 0;
        return;
    }

    std::string filename = get_segment_filename(currentSegmentIndex);
    if (!std::filesystem::exists(filename)) {
        std::cerr << "[disk warn] Missing log file " << filename << ". Resetting offset.\n";
        currentSegmentIndex = 0;
        currentOffset = 0;
    }
    else {
        std::cerr << "[debug] offset: segment=" << currentSegmentIndex << ", offset=" << currentOffset << std::endl;
    }
}

void DiskHandler::save_offset() const {
    std::ofstream file(baseName + ".meta", std::ios::trunc);
    if (!file) {
        std::cerr << "[disk error] save_offset error" << std::endl;
        return;
    }

    file << currentSegmentIndex << ' ' << currentOffset;
    file.flush();
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