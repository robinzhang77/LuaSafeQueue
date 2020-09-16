/**********************************************************************************************
* 模块名称：
*    基本于共享存的无锁对列，lua版本（linux）
*
* 该lua扩展模块包含如下几个功能：
*    1.push接口，可以接收lua传来的table，然后序列化为二进制字节流,并将这个二进制流存入队列
*    2.pop接口，可以从队列中取出字节流里还原lua的table，并返回给lua
*    3.一个环形的无锁队列，针对一个生产者一个消费者来说
*    4.与sharedMemory结合，把队列生成在共享内存中，这样即使进程宕机，数据也不会丢失
*
* 编译 :
*    g++ -fPIC -shared -llua -g luasafequene.c -o safequene.so -I/usr/local/lua5.1/include
*    
* 开发日期：
*    26/08/2019
*
* 作者：
*    张小斌
***********************************************************************************************/


extern "C"
{
    #include "lua.h"
    #include "lauxlib.h"
}

#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/types.h>
#include <fcntl.h>
#include <sys/stat.h>


#if !defined(LUA_VERSION_NUM) || (LUA_VERSION_NUM < 501)
#include "compat-5.1.h"
#endif


//////////////////////////////////////////////////////////////////////////
const int MAX_COUNT = 1024;
const int MAX_MASK = MAX_COUNT - 1;
const int MAX_NODE_SIZE = 1024;
const int MAX_BUFF_SIZE = MAX_NODE_SIZE * MAX_COUNT;
struct single_node_t
{
    char buff[MAX_NODE_SIZE];
};
single_node_t * g_pList = NULL;
unsigned int *g_pReadIdx = NULL;
unsigned int *g_pWriteIdx = NULL;

char g_szBuff[MAX_NODE_SIZE] = { 0 };
int g_nBuffPos = 0;

//////////////////////////////////////////////////////////////////////////
//无锁安全队列（一对一的生产消费关系，一对多或是多对多不安全，其实实际业务也都只是一对g的）
bool init_list()
{
    const char *szFileName = "/dev/shm_lua_list";
    int fd = open(szFileName, O_CREAT | O_RDWR);
    if (fd < 0)
    {
        return false;
    }

    int nSingleNodeSize = sizeof(single_node_t);
    int ret = ftruncate(fd, MAX_BUFF_SIZE);
    if (ret < 0)
    {
        close(fd);
        return false;
    }

    struct stat statbuf;
    ret = stat(szFileName, &statbuf);

    if (ret < 0)
    {
        close(fd);
        return false;
    }

    g_pList = (single_node_t*)mmap(NULL, statbuf.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (g_pList == MAP_FAILED)
    {
        perror("mmap");
        close(fd);
        return false;
    }

    close(fd);

    return true;
}

bool init_list_control()
{
    const char *szFileName = "/dev/shm_lua_ctrl";
    int n = access(szFileName, 0);
    
    int fd = open(szFileName, O_CREAT | O_RDWR);
    if (fd < 0)
    {
        return false;
    }

    int ret = ftruncate(fd, 8);
    if (ret < 0)
    {
        close(fd);
        return false;
    }

    struct stat statbuf;
    ret = stat(szFileName, &statbuf);

    if (ret < 0)
    {
        close(fd);
        return false;
    }

    unsigned int *pInt = (unsigned int*)mmap(NULL, statbuf.st_size, PROT_READ | PROT_WRITE, MAP_SHARED, fd, 0);
    if (pInt == MAP_FAILED)
    {
        perror("mmap shm_lua_ctrl");
        close(fd);
        return false;
    }

    close(fd);

    g_pReadIdx = pInt;
    pInt++;
    g_pWriteIdx = pInt;

    if (n == -1)
    {
        printf("init read/write idx to 0\n");
        *g_pReadIdx = 0;
        *g_pWriteIdx = 0;
    }


    return true;
}

//只更新写索引
bool push_to_list(void *pData, int nLen)
{
    

    if (nLen > MAX_NODE_SIZE)
    {
        return false;
    }

    if (*g_pWriteIdx - *g_pReadIdx < MAX_COUNT)
    {
        int nIdx = (*g_pWriteIdx) & MAX_MASK;
        void *pDes = (void*)(g_pList + nIdx);
        memset(pDes, 0, MAX_NODE_SIZE);
        int nSize = nLen < MAX_NODE_SIZE ? nLen : MAX_NODE_SIZE;
        memcpy(pDes, pData, nSize);
        (*g_pWriteIdx)++;

        printf("readIdx:%d, writeIdx:%d\n", *g_pReadIdx, *g_pWriteIdx);

        return true;
    }

    return false;
}

//只更新读索引
void * pop_from_list()
{
    void *pData = NULL;

    if (*g_pWriteIdx != *g_pReadIdx)
    {
        int nIdx = (*g_pReadIdx) & MAX_MASK;
        pData = (void*)(g_pList + nIdx);
        (*g_pReadIdx)++;
    }

    printf("readIdx:%d, writeIdx:%d\n", *g_pReadIdx, *g_pWriteIdx);

    return pData;
}

//////////////////////////////////////////////////////////////////////////
//流操作函数
static void zero_buff()
{
    memset(g_szBuff, 0, sizeof(g_szBuff));
    int g_nBuffPos = 0;
}

static void reset_buff_pos()
{
    g_nBuffPos = 0;
}

template<typename T>
static void write(T t)
{
    if (g_nBuffPos + sizeof(t) >= sizeof(g_szBuff))
    {
        printf("\n write error: g_buff size no enought!!!\n");
        return;
    }

    memcpy((void*)(g_szBuff + g_nBuffPos), (void*)&t, sizeof(T));
    g_nBuffPos += sizeof(T);
}

static void write_string(const char *szVal)
{
    unsigned short nLen = strlen(szVal)+1;
    if (g_nBuffPos + nLen + sizeof(unsigned short) >= sizeof(g_szBuff))
    {
        printf("\n write_string error: g_buff size no enought!!!\n");
        return;
    }

    memcpy((void*)(g_szBuff + g_nBuffPos), (void*)&nLen, sizeof(unsigned short));
    memcpy((void*)(g_szBuff + g_nBuffPos + sizeof(unsigned short)), (void*)szVal, nLen);

    g_nBuffPos += (nLen + sizeof(unsigned short));
}

static const char * read_string()
{
    if (g_nBuffPos + sizeof(unsigned short) >= sizeof(g_szBuff))
    {
        printf("\n read_string error: g_buff size no enought!!!\n");
        return NULL;
    }

    unsigned short nLen = 0;
    memcpy((void*)&nLen, (void*)(g_szBuff+g_nBuffPos), sizeof(unsigned short));
    g_nBuffPos += sizeof(unsigned short);

    if (g_nBuffPos + nLen >= sizeof(g_szBuff))
    {
        printf("\n read_string error: g_buff size no enought!!!\n");
        return NULL;
    }
    
    const char *szVal = g_szBuff + g_nBuffPos;
    g_nBuffPos += nLen;

    return szVal;
}

template<typename T>
static void read(T& t)
{
    if (g_nBuffPos + sizeof(t) >= sizeof(g_szBuff))
    {
        printf("\n read error: g_buff size no enought!!!\n");
        return;
    }

    memcpy((void*)&t, (void*)(g_szBuff + g_nBuffPos), sizeof(T));
    g_nBuffPos += sizeof(T);
}

//////////////////////////////////////////////////////////////////////////
//一个sleep的测试接口
static int sleep_c(lua_State *L)
{
    int nNumArg = lua_gettop(L);
    if (nNumArg == 1)
    {
       lua_Integer nVal = lua_tointeger(L, 1);
       sleep(nVal);
    }
    return 0;
}

//////////////////////////////////////////////////////////////////////////
//读取lua的table写入流
static void read_lua_table(lua_State * L, int nIdx)
{
    while (lua_next(L, nIdx))
    {
        int nKeyType = lua_type(L, -2);
        int nValType = lua_type(L, -1);

        //写入key类型
        write(nKeyType);

        //写入key
        switch (nKeyType)
        {
            case LUA_TSTRING:
            {
                const char * szKey = lua_tostring(L, -2);
                write_string(szKey);
            }
            break;
            case LUA_TNUMBER:
            {
                lua_Number nKey = lua_tonumber(L, -2);
                write(nKey);
            }
            break;
        }

        //写入value类型 与 value的数据
        write(nValType);
        switch (nValType)
        {
            case LUA_TNUMBER:
            {
                lua_Number nVal = lua_tonumber(L, -1);
                write(nVal);
            }
            break;
            case LUA_TSTRING:
            {
                const char *szVal = lua_tostring(L, -1);
                write_string(szVal);
            }
            break;
            case LUA_TNIL:
            {

            }
            break;
            case LUA_TBOOLEAN:
            {
                int nVal = lua_toboolean(L, -1);
                write(nVal);
            }
            break;
            case LUA_TTABLE:
            {
                lua_pushnil(L);
                read_lua_table(L, -2);
                write(LUA_TTHREAD + 1); //表示table结束符
            }
            break;
        }

        //把栈顶的val移除
        lua_pop(L, 1);

        //printf("--------stack size = %d\n", lua_gettop(L));
    }
}

static int push_c(lua_State *L)
{
    zero_buff();

    lua_pushnil(L);

    read_lua_table(L, 1);

    push_to_list(g_szBuff, g_nBuffPos);
        
    lua_pop(L, lua_gettop(L));

    return 0;
}


static void write_lua_table(lua_State * L)
{
    lua_newtable(L);

    bool bVal = true;

    while (bVal)
    {
        int nKeyType = 0;

        read(nKeyType);

        switch (nKeyType)
        {
            case LUA_TNUMBER:
            {
                lua_Number nValue = 0;
                read(nValue);
                lua_pushnumber(L, nValue);
            }
            break;
            case LUA_TSTRING:
            {
                const char *szVal = read_string();
                lua_pushstring(L, szVal);
            }
            break;
            case (LUA_TTHREAD + 1):
            {
            lua_settable(L, -3);
            continue;
            }
            break;
            default:
            {
                bVal = false;
            }
            break;
        }

        int nValType = 0;
        read(nValType);

        switch (nValType)
        {
        case LUA_TNUMBER:
        {
            lua_Number nValue = 0;
            read(nValue);
            lua_pushnumber(L, nValue);
            lua_settable(L, -3);
        }
        break;
        case LUA_TSTRING:
        {
            const char *szVal = read_string();
            lua_pushstring(L, szVal);
            lua_settable(L, -3);
        }
        break;
        case LUA_TTABLE:
        {
            write_lua_table(L);
        }
        break;
        case LUA_TBOOLEAN:
        {
            int nVal = 0;
            read(nVal);
            lua_pushboolean(L, nVal);

        }
        break;
        default:
        {
            //printf("nValType is invalid, %d\n", nValType);
            bVal = false;
        }
        break;
        }
    }
}

static int pop_c(lua_State *L)
{
    void *pData = pop_from_list();
    if (pData)
    {
        memcpy(g_szBuff, pData, sizeof(g_szBuff));
        reset_buff_pos();
        write_lua_table(L);
        return 1;
    }

    return 0;
}





//////////////////////////////////////////////////////////////////////////
//lua模块的注册
static const struct luaL_Reg libs[] = {
    {"sleep", sleep_c},
    {"push", push_c},
    {"pop", pop_c},
    {NULL, NULL} /* then end */
};

//////////////////////////////////////////////////////////////////////////
//模块加载
extern "C"
{
    int luaopen_safequeue(lua_State *L)
    {
        /*libname, funcname, registername must be same*/
        const char *szName = "safequeue";
        luaL_register(L, szName, libs);
        init_list();
        init_list_control();
        return 1;
    }
}

