#include <iostream>
#include <string>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>
#include <atomic>
#include <chrono>

#pragma comment(lib, "Ws2_32.lib")

std::atomic<bool> running(true);

std::string handle_command(const std::string& command) {
    if (command.find("SUBSCRIBE ") == 0) {
        std::string topic = command.substr(10);
        std::cout << "[Client] Subscribed to topic: " << topic << std::endl;
        return "OK\n";
    }
    else if (command.find("PULL") == 0) {
        std::cout << "[Client] Pulling message for current topic." << std::endl;
        return "OK: Dummy message\n";
    }
    else {
        std::cerr << "[Client] Invalid command: " << command << std::endl;
        return "INVALID_CMD\n";
    }
}

void topic_pull_thread(SOCKET sock, const std::string& topic) {
    std::string subscribeCommand = "SUBSCRIBE " + topic + "\n";
    int bytesSent = send(sock, subscribeCommand.c_str(), static_cast<int>(subscribeCommand.length()), 0);
    if (bytesSent == SOCKET_ERROR) {
        std::cerr << "Send failed. Error: " << WSAGetLastError() << std::endl;
        return;
    }
    std::cout << "[Client] Sent subscribe command for topic: " << topic << std::endl;

    while (running) {
        std::this_thread::sleep_for(std::chrono::seconds(2));
        std::string pullCommand = "PULL " + topic + "\n";
        bytesSent = send(sock, pullCommand.c_str(), static_cast<int>(pullCommand.length()), 0);
        if (bytesSent == SOCKET_ERROR) {
            std::cerr << "Send failed. Error: " << WSAGetLastError() << std::endl;
            break;
        }
        std::cout << "[Client] Sent PULL command for topic: " << topic << std::endl;

        char buffer[1024];
        int bytesReceived = recv(sock, buffer, sizeof(buffer), 0);
        if (bytesReceived > 0) {
            std::string receivedMessage(buffer, bytesReceived);
            std::cout << "[Client] Received message: " << receivedMessage << std::endl;
        }
        else if (bytesReceived == 0) {
            std::cout << "Connection closed." << std::endl;
            break;
        }
        else {
            std::cerr << "Receive failed. Error: " << WSAGetLastError() << std::endl;
            break;
        }
    }
}

bool init_connection(SOCKET& sock, const std::string& server_ip, uint16_t port) {
    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(port);

    if (inet_pton(AF_INET, server_ip.c_str(), &serverAddr.sin_addr) <= 0) {
        std::cerr << "Invalid address or address not supported." << std::endl;
        return false;
    }

    sock = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
    if (sock == INVALID_SOCKET) {
        std::cerr << "Socket creation failed. Error: " << WSAGetLastError() << std::endl;
        return false;
    }

    if (connect(sock, (SOCKADDR*)&serverAddr, sizeof(serverAddr)) == SOCKET_ERROR) {
        std::cerr << "Connection failed. Error: " << WSAGetLastError() << std::endl;
        closesocket(sock);
        return false;
    }

    return true;
}

int main() {
    WSADATA wsaData;
    if (WSAStartup(MAKEWORD(2, 2), &wsaData) != 0) {
        std::cerr << "WSAStartup failed." << std::endl;
        return 1;
    }

    SOCKET clientSocket;
    std::string server_ip = "127.0.0.1";
    uint16_t port = 12345;

    if (!init_connection(clientSocket, server_ip, port)) {
        std::cerr << "Failed to connect to the server." << std::endl;
        WSACleanup();
        return 1;
    }
    std::cout << "Connected to the server." << std::endl;

    std::string topic = "topic1";
    std::thread pullThread(topic_pull_thread, clientSocket, topic);
    pullThread.join();

    closesocket(clientSocket);
    WSACleanup();

    return 0;
}