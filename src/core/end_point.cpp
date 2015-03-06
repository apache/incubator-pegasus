
# ifdef _WIN32

# define _WINSOCK_DEPRECATED_NO_WARNINGS 1
# include <WinSock2.h>
# pragma comment(lib, "ws2_32.lib")

# else
# include <sys/socket.h>
# include <netdb.h>
# include <arpa/inet.h>
# endif

# include <rdsn/internal/end_point.h>
# include <mutex>

namespace rdsn {

const end_point end_point::INVALID;

end_point::end_point(const char* str, uint16_t p)
{
    static std::once_flag flag;
    static bool flag_inited = false;
    if (!flag_inited)
    {
        std::call_once(flag, [&]() 
        {
#ifdef _WIN32
            WSADATA wsaData;
            WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
            flag_inited = true;
        });
    }

    port = p;
    name = std::string(str);

    sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    addr.sin_family=AF_INET;

    if ((addr.sin_addr.s_addr = inet_addr(str)) == (unsigned int)(-1))
    {
        hostent* hp = gethostbyname(str);
        if (hp != 0) 
        {
            memcpy((void*)&(addr.sin_addr.s_addr), (const void*)hp->h_addr, (size_t)hp->h_length);
        }
    }

    // network order
    ip = (uint32_t)(addr.sin_addr.s_addr);
}

} // end namespace
