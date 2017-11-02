#ifndef _ZCUT_DBG_H_
#define _ZCUT_DBG_H_

#define ZLOG_DEBUG  0x00000001u
#define ZLOG_INFO   0x00000002u
#define ZLOG_MSG    0x00000004u
#define ZLOG_WARN   0x00000008u
#define ZLOG_ERR    0x00000010u
#define ZLOG_CRIT   0x00000020u

#define ZLOG_INFO_D (ZLOG_DEBUG | ZLOG_INFO)
#define ZLOG_MSG_D  (ZLOG_DEBUG | ZLOG_INFO | ZLOG_MSG)
#define ZLOG_WARN_D (ZLOG_DEBUG | ZLOG_INFO | ZLOG_MSG | ZLOG_WARN)
#define ZLOG_ERR_D  (ZLOG_DEBUG | ZLOG_INFO | ZLOG_MSG | ZLOG_WARN | ZLOG_ERR)
#define ZLOG_CRIT_D (ZLOG_DEBUG | ZLOG_INFO | ZLOG_MSG | ZLOG_WARN | ZLOG_ERR | ZLOG_CRIT)
#define ZLOG_ALL    (ZLOG_DEBUG | ZLOG_INFO | ZLOG_MSG | ZLOG_WARN | ZLOG_ERR | ZLOG_CRIT)
#define ZLOG_STDERR (ZLOG_MSG | ZLOG_WARN | ZLOG_ERR | ZLOG_CRIT)

#define ERR_DEFAULT         0x7F
#define ERR_END_OF_STREAM   0x70

#define ZLOG(LEVEL, FORMAT, ...) \
    do \
    { \
        if (g_zlog_level & LEVEL) \
        { \
            if (LEVEL & ZLOG_STDERR) \
                fprintf(stderr, "%s (%u) " FORMAT, __func__, __LINE__, ## __VA_ARGS__); \
            else if (LEVEL & ZLOG_INFO_D) \
                fprintf(stderr, "%s (%u) " FORMAT, __func__, __LINE__, ## __VA_ARGS__); \
        } \
    } while (0)

extern uint32_t g_zlog_level;

#endif // _ZCUT_DBG_H_

