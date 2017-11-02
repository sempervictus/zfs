#ifndef _ZCUT_SPLIT_H_
#define _ZCUT_SPLIT_H_

#include "sys/zfs_ioctl.h"
#include "zcut.h"
#include "zcut_partname.h"

#define FILENAME_MAX_LEN        256

typedef enum split_index_e
{
    SPLIT_FIRST,
    SPLIT_MID,
    SPLIT_LAST
} split_index_t;

/**
 * It's legal to use the same name for struct and typedefed struct.
 * e.g.
 * typedef struct split_param_t {} split_param_t;
 */
typedef struct split_param_s
{
    uint64_t split_size;
    uint32_t split_blocks;
    boolean  split_for_snap;
    uint32_t stream_type;
    uint32_t reserved;
} split_param_t;

struct split_handler_s;
typedef struct split_handler_s split_handler_t;

split_handler_t *init_split_handler(split_param_t *param);
void free_split_handler(split_handler_t *spliter);
void dump_split_handler(split_handler_t *spliter);
dmu_replay_record_t next_split_begin(dmu_replay_record_t *pcurr_begin, split_handler_t *spliter, boolean needswap, boolean is_last);
void split_counter(split_handler_t *spliter, uint64_t bytes, uint32_t blocks);
void reset_split_counter(split_handler_t *spliter);
void set_first_split(split_handler_t *spliter, boolean b);
boolean split_for_snap(split_handler_t *spliter);
boolean need_to_split(split_handler_t *spliter);
boolean is_first_split(split_handler_t *spliter);

#endif // _ZCUT_SPLIT_H_

