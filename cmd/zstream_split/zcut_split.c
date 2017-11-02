#include <sys/queue.h>
#include "sys/zfs_ioctl.h"
#include "zcut_split.h"
#include "zcut_dbg.h"

#define NULL_FD     (-1)

typedef struct split_snap_s
{
    uint64_t fromguid;
    uint64_t toguid;
    char toname[MAXNAMELEN];
    char filename[FILENAME_MAX_LEN];
    TAILQ_ENTRY(split_snap_s) link;
} split_snap_t;

struct split_handler_s
{
    uint64_t split_bytes_max;
    uint64_t split_bytes;
    uint32_t split_blocks_max;
    uint32_t split_blocks;
    uint32_t split_file_idx;
    uint32_t split_snap_idx;
    boolean  first_split;
    boolean  split_for_snap;
    int      wrfd;
    int      stream_type;
    TAILQ_HEAD(split_snap_head, split_snap_s) split_snaps;
};

static uint64_t generate_guid(uint64_t base_guid, uint32_t idx);
static char *generate_toname(const char *base_toname, int idx, char *new_toname);
static split_snap_t *next_split_snap(split_handler_t *spliter, uint64_t toguid, const char *toname);
static int next_split_file(split_handler_t *spliter);
void dump_split_snap(split_snap_t *split);

/**********************************************
 *
 * Local functions
 *
 **********************************************/
uint64_t
generate_guid(uint64_t base_guid, uint32_t idx)
{
    int i;
    uint64_t guid = 0;
    uint16_t guid_words[4] = {0};

    guid_words[0] = ((base_guid >> 48) & 0xFFFF) + ((idx >> 24) & 0xFF);
    guid_words[1] = ((base_guid >> 32) & 0xFFFF) + ((idx >> 16) & 0xFF);
    guid_words[2] = ((base_guid >> 16) & 0xFFFF) + ((idx >> 8) & 0xFF);
    guid_words[3] = (base_guid & 0xFFFF) + (idx & 0xFF);

    for (guid = 0, i = 0; i < 4; ++i)
        guid = (guid << 16) + guid_words[i];

    return guid;
}

char *
generate_toname(const char *base_toname, int idx, char *new_toname)
{
    char toname[MAXNAMELEN] = {0};
    char *tosnap = NULL;

    strcpy(toname, base_toname);

    if ((tosnap = strchr(toname, '@')) == NULL)
    {
        ZLOG(ZLOG_WARN, "Invalid stream (bad snapshot name)\n");
        return NULL;
    }

    *tosnap = '\0';
    tosnap++;

    snprintf(new_toname, MAXNAMELEN, "%s@%s_part_%u", toname, tosnap, idx);

    return new_toname;
}

split_snap_t *
next_split_snap(split_handler_t *spliter, uint64_t toguid, const char *toname)
{
    split_snap_t *ss = NULL;

    ss = malloc(sizeof(split_snap_t));

    if (NULL == ss)
    {
        ZLOG(ZLOG_WARN, "Failed to allocate memory for split_snap_t\n");
        return NULL;
    }

    // snap's or file's index starts from 1
    spliter->split_snap_idx++;

    bzero(ss, sizeof(split_snap_t));
    ss->toguid = generate_guid(toguid, spliter->split_snap_idx);
    generate_toname(toname, spliter->split_snap_idx, ss->toname);

    TAILQ_INSERT_TAIL(&spliter->split_snaps, ss, link);

    if (spliter->split_for_snap)
    {
        g_wrfd = next_split_file(spliter);
    }

    snprintf(ss->filename, FILENAME_MAX_LEN, SPLIT_FILENAME_FORMAT, spliter->split_file_idx);

    return ss;
}

int
next_split_file(split_handler_t *spliter)
{
    char filename[FILENAME_MAX_LEN] = {0};
    
    // snap's or file's index starts from 1
    spliter->split_file_idx++;

    /* dump zstrem to stdout */
    if (g_stream_to_stdout)
    {
        /* Always use stdout */
        spliter->wrfd = STDOUT_FILENO; /* stdout */
        return spliter->wrfd;
    }

    if (spliter->wrfd > 0)
        close(spliter->wrfd);

    snprintf(filename, sizeof(filename), SPLIT_FILENAME_FORMAT, spliter->split_file_idx);
    spliter->wrfd = open(filename, O_RDWR | O_APPEND | O_CREAT, S_IRUSR | S_IWUSR | S_IRGRP | S_IWGRP | S_IROTH);

    return spliter->wrfd;
}

void
dump_split_snap(split_snap_t *split)
{
    ZLOG(ZLOG_DEBUG, "      fromguid = %lx\n", split->fromguid);
    ZLOG(ZLOG_DEBUG, "      toguid = %lx\n", split->toguid);
    ZLOG(ZLOG_DEBUG, "      toname = %s\n", split->toname);
    ZLOG(ZLOG_DEBUG, "      filename = %s\n", split->filename);
}

/**********************************************
 *
 * Exported functions
 *
 **********************************************/
split_handler_t *
init_split_handler(split_param_t *param)
{
    split_handler_t *spliter = NULL;

    spliter = malloc(sizeof(split_handler_t));

    if (NULL == spliter)
        return NULL;

    bzero(spliter, sizeof(split_handler_t));
    spliter->split_bytes_max = param->split_size;
    spliter->split_blocks_max = param->split_blocks;
    spliter->first_split = TRUE;
    spliter->split_for_snap = param->split_for_snap;
    spliter->wrfd = NULL_FD;
    spliter->stream_type = param->stream_type;

    TAILQ_INIT(&spliter->split_snaps);

    return spliter;
}

void
free_split_handler(split_handler_t *spliter)
{
    split_snap_t *ss = NULL;

    while ((ss = TAILQ_FIRST(&spliter->split_snaps)) != NULL)
    {
        TAILQ_REMOVE(&spliter->split_snaps, ss, link);
        free(ss);
    }

    free(spliter);
}

void
dump_split_handler(split_handler_t *spliter)
{
    int i = 0;
    split_snap_t *ss = NULL;

    ZLOG(ZLOG_DEBUG, "[split handler]\n");
    ZLOG(ZLOG_DEBUG, "  split_bytes_max = %lu\n", spliter->split_bytes_max);
    ZLOG(ZLOG_DEBUG, "  split_bytes = %lu\n", spliter->split_bytes);
    ZLOG(ZLOG_DEBUG, "  split_blocks_max = %u\n", spliter->split_blocks_max);
    ZLOG(ZLOG_DEBUG, "  split_blocks = %u\n", spliter->split_blocks);
    ZLOG(ZLOG_DEBUG, "  split_file_idx = %u\n", spliter->split_file_idx);
    ZLOG(ZLOG_DEBUG, "  split_snap_idx = %u\n", spliter->split_snap_idx);
    ZLOG(ZLOG_DEBUG, "  first_split = %u\n", spliter->first_split);
    ZLOG(ZLOG_DEBUG, "  split_for_snap = %u\n", spliter->split_for_snap);
    ZLOG(ZLOG_DEBUG, "  stream_type = %u\n", spliter->stream_type);

    TAILQ_FOREACH(ss, &spliter->split_snaps, link)
    {
        ZLOG(ZLOG_DEBUG, "  [%d]\n", i++);
        dump_split_snap(ss);
    }
}

dmu_replay_record_t
next_split_begin(
    dmu_replay_record_t *pcurr_begin,
    split_handler_t *spliter,
    boolean needswap,
    boolean is_last)
{
    dmu_replay_record_t drr = *pcurr_begin;
    struct drr_begin *drrb = &drr.drr_u.drr_begin;
    split_snap_t *ss = NULL;
    split_snap_t *ss_prev = NULL;

    ss = next_split_snap(spliter, drrb->drr_toguid, drrb->drr_toname);
    if (NULL == ss)
    {
        ZLOG(ZLOG_MSG, "Failed to create next split snapshot\n");
        return drr;
    }

    if (is_last)
    {
        ss->toguid = drrb->drr_toguid;
        strcpy(ss->toname, drrb->drr_toname);
        spliter->split_snap_idx = 0;
        spliter->first_split = TRUE;
    }
    else
    {
        drrb->drr_toguid = ss->toguid;
        strcpy(drrb->drr_toname, ss->toname);
        spliter->first_split = FALSE;
    }

    ss_prev = TAILQ_PREV(ss, split_snap_head, link);
    if (ss_prev)
        ss->fromguid = ss_prev->toguid;
    else
    {
        ss->fromguid = drrb->drr_fromguid;
        spliter->first_split = TRUE;
    }

    drrb->drr_fromguid = ss->fromguid;
    reset_split_counter(spliter);

    return drr;
}

void
split_counter(split_handler_t *spliter, uint64_t bytes, uint32_t blocks)
{
    spliter->split_bytes += bytes;
    spliter->split_blocks += blocks;
}

void
reset_split_counter(split_handler_t *spliter)
{
    spliter->split_bytes = 0;
    spliter->split_blocks = 0;
}

boolean
split_for_snap(split_handler_t *spliter)
{
    return spliter->split_for_snap;
}

boolean
need_to_split(split_handler_t *spliter)
{
    if (spliter->split_bytes >= spliter->split_bytes_max ||
        spliter->split_blocks >= spliter->split_blocks_max)
        return TRUE;

    return FALSE;
}

boolean
is_first_split(split_handler_t *spliter)
{
    return spliter->first_split;
}

void
set_first_split(split_handler_t *spliter, boolean b)
{
    spliter->first_split = b;
}
