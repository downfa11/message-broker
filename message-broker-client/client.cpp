#include <iostream>
#include <string>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>
#include <atomic>
#include <chrono>
#include <mutex>
#include <vector>

#pragma comment(lib, "Ws2_32.lib")

std::atomic<bool> running(true);
std::mutex cout_mutex;

void topic_pull_thread(SOCKET sock, const std::string& topic) {

    std::string subscribeCommand = "SUBSCRIBE " + topic;
    int bytesSent = send(sock, subscribeCommand.c_str(), static_cast<int>(subscribeCommand.length()), 0);

    if (bytesSent == SOCKET_ERROR) {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cerr << "[error] Topic subscribe: " << WSAGetLastError() << std::endl;
        return;
    }

    {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cout << "[info] subscirbed Topic: " << topic << std::endl;
    }

    while (running) {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));

        std::string pullCommand = "PULL " + topic;
        bytesSent = send(sock, pullCommand.c_str(), static_cast<int>(pullCommand.length()), 0);


        if (bytesSent == SOCKET_ERROR) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "[error] Message pull: " << WSAGetLastError() << std::endl;
            break;
        }

        char buffer[1024];
        int bytesReceived = recv(sock, buffer, sizeof(buffer), 0);

        if (bytesReceived > 0) {
            std::string receivedMessage(buffer, bytesReceived);

            if (receivedMessage != "NO_MESSAGES") {
                std::lock_guard<std::mutex> lock(cout_mutex);
                std::cout << "[info] message: " << receivedMessage << std::endl;
            }
        }
        else if (bytesReceived == 0) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "[error] Connection closed." << std::endl;
            break;
        }
        else {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "[error] receive failed: " << WSAGetLastError() << std::endl;
            break;
        }
    }
}

bool init_connection(SOCKET& sock, const std::string& server_ip, uint16_t port) {
    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);

    if (inet_pton(AF_INET, server_ip.c_str(), &serverAddr.sin_addr) <= 0) {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cerr << "[error] Invalid Broker Address." << std::endl;
        return false;
    }

    sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == INVALID_SOCKET) {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cerr << "[error] socket WSAGetLastError: " << WSAGetLastError() << std::endl;
        return false;
    }

    if (connect(sock, (SOCKADDR*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cerr << "[error] connect WSAGetLastError: " << WSAGetLastError() << std::endl;
        closesocket(sock);
        return false;
    }

    return true;
}

int main() {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::lock_guard<std::mutex> lock(cout_mutex);
        std::cerr << "[error] WSAStartup failed." << std::endl;
        return 1;
    }

    int count;
    std::cout << "Input thread count: ";
    std::cin >> count;

    std::string server_ip = "127.0.0.1";
    uint16_t port = 12345;
    std::string topic = "topic1";

    std::vector<std::thread> threads;
    std::vector<SOCKET> sockets;

    for (int i = 0; i < count; ++i) {
        SOCKET clientSocket;
        if (!init_connection(clientSocket, server_ip, port)) {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cerr << "[error] Connect failed for broker #" << i << std::endl;
            continue;
        }

        {
            std::lock_guard<std::mutex> lock(cout_mutex);
            std::cout << "[info] Connect successed for broker #" << i << std::endl;
        }

        threads.emplace_back(topic_pull_thread, clientSocket, topic);
        sockets.push_back(clientSocket);
    }

    for (auto& t : threads) {
        if (t.joinable()) t.join();
    }

    for (SOCKET s : sockets) {
        closesocket(s);
    }

    WSACleanup();
    return 0;
}