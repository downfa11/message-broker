#include <iostream>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <thread>

#pragma comment(lib, "ws2_32.lib")

void recv_thread(SOCKET sock) {
    char buffer[1024];
    DWORD bytesReceived = 0;
    DWORD flags = 0;
    WSAOVERLAPPED overlapped = { 0 };
    WSABUF wsabuf;
    wsabuf.buf = buffer;
    wsabuf.len = sizeof(buffer);

    while (true) {
        int result = WSARecv(sock, &wsabuf, 1, &bytesReceived, &flags, &overlapped, NULL);

        if (result == SOCKET_ERROR) {
            int error = WSAGetLastError();
            if (error == WSA_IO_PENDING) {
                Sleep(100);
                continue;
            }
            else {
                std::cerr << "recv failed. Error: " << error << std::endl;
                break;
            }
        }

        if (bytesReceived == 0) {
            std::cout << "Server closed.\n";
            break;
        }

        buffer[bytesReceived] = '\0'; // null-terminate
        std::cout << "Received: " << buffer << std::endl;
    }
}

int main() {
    WSADATA wsaData;
    WSAStartup(MAKEWORD(2, 2), &wsaData);

    SOCKET sock = socket(AF_INET, SOCK_STREAM, 0);

    sockaddr_in serverAddr;
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(12345);
    inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr);

    if (connect(sock, (sockaddr*)&serverAddr, sizeof(serverAddr)) < 0) {
        int error = WSAGetLastError();
        std::cerr << "Connect failed. Error: " << error << std::endl;
        return 1;
    }

    std::cout << "Connect success." << std::endl;

    const char* subCmd = "SUBSCRIBE topic1\n";
    send(sock, subCmd, strlen(subCmd), 0);

    std::thread recvThread(recv_thread, sock);

    while (true) {
        const char* pullCmd = "PULL\n";
        send(sock, pullCmd, strlen(pullCmd), 0);

        Sleep(1000);
    }

    recvThread.join(); 

    closesocket(sock);
    WSACleanup();
    return 0;
}
