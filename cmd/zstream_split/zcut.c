#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <time.h>

#include "zcut_dbg.h"
#include "zcut_drr.h"
#include "zcut_util.h"
#include "zcut_prop.h"
#include "zcut_partname.h"
#include "sys/zfs_ioctl.h"
#include "zfs_fletcher.h"

#define SPLIT_BYTES         (1 << 27)   // 128 MB
#define SPLIT_BLOCKS        0x7fffffff

int g_wrfd = 0;
int g_rdfd = STDIN_FILENO;
int g_dump_drr = 0;
int g_stream_to_stdout = 0;  /* default is not dump the stream to stdout */
char g_resume_snapshot_name[64] = {0}; /* snapshot to resume */
int g_output_stage = 1; /* switch of write or skip output */
boolean g_rename_snapshot = FALSE;
uint32_t g_zlog_level = ZLOG_ALL;
split_handler_t *g_spliter = NULL;
zio_cksum_t g_overall_cksum = {{0}};

static int
parse_single(dmu_replay_record_t *pdrr_begin, boolean needswap)
{
    int ret = 0;
    dmu_replay_record_t drr = *pdrr_begin;
    zio_cksum_t zcksum = {{0}};
    boolean is_end = FALSE;

    do
    {
        if (IS_DRR_BEGIN(drr.drr_type) || IS_DRR_END(drr.drr_type))
            ret = split_handler(NULL, NULL, NULL, pdrr_begin, &drr,
                                g_spliter, &zcksum, needswap);
        else
            ret = drr_handler(&drr, g_spliter, &zcksum, needswap);

        if (ret || is_end)
            break;

        bzero(&drr, DRR_SIZE);
        ret = read_drr(&drr, &g_overall_cksum, needswap);
        if (ret <= 0)
        {
            ZLOG(ZLOG_MSG, "Failed to read drr\n");
            break;
        }

        if (IS_DRR_END(drr.drr_type))
        {
            is_end = TRUE;
            continue;
        }

        if (need_to_split(g_spliter))
        {
            ret = split_handler(NULL, NULL, NULL, pdrr_begin, &drr, g_spliter, &zcksum, needswap);
            if (ret)
                break;
        }
    } while (1);

    close(g_wrfd);

    return ret;
}

static int
parse_package(dmu_replay_record_t *ppkg_begin, boolean needswap)
{
    int ret = 0;
    /**
     * Structures are not initialized. Add `= {0}`.
     */
    dmu_replay_record_t drr;
    dmu_replay_record_t curr_begin;
    dmu_replay_record_t pkg_end;
    void *prop = NULL;
    zio_cksum_t zcksum = {{0}};
    boolean is_end = FALSE;

    // first begin - end with properties
    ret = read_pkg_begin_end(ppkg_begin, &pkg_end, &prop, needswap);
    if (ret)
        return ret;

    reset_split_counter(g_spliter);
    bzero(&g_overall_cksum, sizeof(zio_cksum_t));

    ret = read_drr(&drr, &g_overall_cksum, needswap);
    if (ret <= 0)
    {
        ZLOG(ZLOG_MSG, "Failed to read second BEGIN\n");
        return ret;
    }

    curr_begin = drr;

    do
    {
        if (IS_DRR_BEGIN(drr.drr_type) || IS_DRR_END(drr.drr_type))
            ret = split_handler(ppkg_begin, &pkg_end, prop, &curr_begin, &drr,
                                g_spliter, &zcksum, needswap);
        else
            ret = drr_handler(&drr, g_spliter, &zcksum, needswap);

        if (ret)
            break;

        bzero(&drr, DRR_SIZE);
        ret = read_drr(&drr, &g_overall_cksum, needswap);
        if (ret <= 0)
        {
            ZLOG(ZLOG_MSG, "Failed to read drr\n");
            break;
        }

        if (IS_DRR_END(drr.drr_type))
        {
            if (is_end)
                break;

            is_end = TRUE;
            continue;
        }

        is_end = FALSE;

        if (need_to_split(g_spliter))
        {
            ret = split_handler(ppkg_begin, &pkg_end, prop, &curr_begin, &drr,
                                g_spliter, &zcksum, needswap);
            if (ret)
                break;
        }
    } while (1);

    close(g_wrfd);

    return ret;
}

static int
parse_package_stream(dmu_replay_record_t *ppkg_begin, boolean needswap)
{
    int ret = 0;
    dmu_replay_record_t drr;
    dmu_replay_record_t drrtmp;
    dmu_replay_record_t curr_begin;
    dmu_replay_record_t pkg_end;
    dmu_replay_record_t *pdb = NULL;
    void *prop = NULL;
    zio_cksum_t zcksum = {{0}};
    zio_cksum_t overall_cksum_tmp = {{0}};
    boolean is_end = FALSE;

    // first begin - end with properties
    ret = read_pkg_begin_end(ppkg_begin, &pkg_end, &prop, needswap);
    if (ret)
        return ret;

    reset_split_counter(g_spliter);
    bzero(&g_overall_cksum, sizeof(zio_cksum_t));

    ret = read_drr(&drr, &g_overall_cksum, needswap);
    if (ret <= 0)
    {
        ZLOG(ZLOG_MSG, "Failed to read second BEGIN\n");
        return ret;
    }

    curr_begin = drr;
    pdb = ppkg_begin;
    set_first_split(g_spliter, FALSE);

    do
    {
        if (IS_DRR_BEGIN(drr.drr_type) || IS_DRR_END(drr.drr_type))
        {
            ret = split_handler(pdb, &pkg_end, prop, &curr_begin, &drr,
                                g_spliter, &zcksum, needswap);
            pdb = NULL;
        }
        else
            ret = drr_handler(&drr, g_spliter, &zcksum, needswap);

        if (ret || is_end)
            break;

        bzero(&drr, DRR_SIZE);
        ret = read_drr(&drr, &g_overall_cksum, needswap);
        if (ret <= 0)
        {
            ZLOG(ZLOG_MSG, "Failed to read drr\n");
            break;
        }

        if (IS_DRR_END(drr.drr_type))
        {
            overall_cksum_tmp = g_overall_cksum;
            bzero(&g_overall_cksum, sizeof(zio_cksum_t));
            ret = read_drr(&drrtmp, &g_overall_cksum, needswap);
            if (ret <= 0)
            {
                ZLOG(ZLOG_MSG, "Failed to read next BEGIN/END\n");
                return ret;
            }
            if (IS_DRR_END(drrtmp.drr_type))
            {
                g_overall_cksum = overall_cksum_tmp;
                is_end = TRUE;
                pdb = ppkg_begin;
                set_first_split(g_spliter, FALSE);
            }
            else
            {
                ret = split_handler(pdb, &drrtmp, prop, &curr_begin, &drr,
                                    g_spliter, &zcksum, needswap);
                if (ret)
                    break;
                drr = drrtmp;
                curr_begin = drrtmp;
            }
            continue;
        }

        if (need_to_split(g_spliter))
        {
            ret = split_handler(pdb, &pkg_end, prop, &curr_begin, &drr,
                                g_spliter, &zcksum, needswap);
            if (ret)
                break;
        }
    } while (1);

    close(g_wrfd);

    return ret;
}


static void
usage(const char *progname)
{
    fprintf(stderr, "usage: %s < <input_stream>\n", progname);
}

int
main(int argc, char **argv)
{
    dmu_replay_record_t drr_begin;          // DRR_SIZE = 312 bytes
    struct drr_begin *drrb = &drr_begin.drr_u.drr_begin;
    uint64_t versioninfo = 0;
    uint64_t featureflags = 0;
    int hdrtype = 0;
    /**
     * A 32bit boolean value is wired to me. If padding is required, use
     * another variable for that. e.g.
     * struct tmp_s {
     *   char padding[3];
     * } tmp_s;
     */
    boolean needswap = FALSE;
    char c;
    /**
     * Recommend syntax:
     * sparam = {
     *  .split_size = 0,
     *  ...
     * }
     */
    split_param_t sparam = {0, SPLIT_BLOCKS, TRUE, 0, 0};

    while ((c = getopt(argc, argv, "vos:Sn:r")) != -1)
    {
        switch (c)
        {
        case 'v':
            g_dump_drr = 1;
        break;
        case 'o':
            g_stream_to_stdout = 1;
        break;
        case 's':
            sparam.split_size = strtoul(optarg, NULL, 10);
        break;
        case 'S':
            /**
             * Per Peikang's description, I suggest to change naming as
             * monolithic or sparse.
             */
            sparam.split_for_snap = FALSE;
        break;
        case 'n':
            if (strlen(optarg) <= 0 || strlen(optarg) >= 64)
            {
                usage(argv[0]);
                return ERR_DEFAULT;
            }
            if (strstr(optarg, SPLIT_TONAME_KEYWORD) != NULL)
            {
                /* resume from intermedium snapshot only */
                strcpy(g_resume_snapshot_name, optarg);
            }
        break;
        case 'r':
            g_rename_snapshot = TRUE;
        break;
        case '?':
        default:
            usage(argv[0]);
            return ERR_DEFAULT;
        }
    }

    if (g_stream_to_stdout && isatty(STDOUT_FILENO))
    {
        ZLOG(ZLOG_WARN, "Stream must not be writen to standard output\n");
        usage(argv[0]);
        return ERR_DEFAULT;
    }

    if (isatty(STDIN_FILENO))
    {
        ZLOG(ZLOG_WARN, "Stream must be read from standard input.\n");
        usage(argv[0]);
        return ERR_DEFAULT;
    }

    if (read_drr(&drr_begin, &g_overall_cksum, needswap) <= 0)
    {
        ZLOG(ZLOG_MSG, "Failed to read first BEGIN\n");
        return ERR_DEFAULT;
    }

    // Compare drr_begin.drr_type (sender byte endian)
    // with DRR_BEGIN (host byte endian)  to
    // determine byte order.
    if (drr_begin.drr_type == DRR_BEGIN &&
        drrb->drr_magic == DMU_BACKUP_MAGIC)
    {
        needswap = FALSE;
        versioninfo = drrb->drr_versioninfo;
    }
    else if (drr_begin.drr_type == BSWAP_32(DRR_BEGIN) &&
             drrb->drr_magic == BSWAP_64(DMU_BACKUP_MAGIC))
    {
        bzero(&g_overall_cksum, sizeof(zio_cksum_t));
        fletcher_4_incremental_byteswap(&drr_begin, DRR_SIZE,
                                        &g_overall_cksum);
        needswap = TRUE;
        versioninfo = BSWAP_64(drrb->drr_versioninfo);
    }
    else
    {
        ZLOG(ZLOG_WARN, "Invalid stream (bad magic number)\n");
        return ERR_DEFAULT;
    }

    ZLOG(ZLOG_DEBUG, "first begin checksum %lx/%lx/%lx/%lx\n",
         g_overall_cksum.zc_word[0], g_overall_cksum.zc_word[1],
         g_overall_cksum.zc_word[2], g_overall_cksum.zc_word[3]);

    hdrtype = DMU_GET_STREAM_HDRTYPE(versioninfo);
    featureflags = DMU_GET_FEATUREFLAGS(versioninfo);
    sparam.stream_type = hdrtype;

    g_spliter = init_split_handler(&sparam);
    if (NULL == g_spliter)
        return ENOMEM;

    if (hdrtype == DMU_SUBSTREAM)
    {
        if (g_stream_to_stdout)
        {
            ZLOG(ZLOG_MSG, "Single stream(\"-i\") to stdout is not supported\n");
            return ERR_DEFAULT;
        }
        parse_single(&drr_begin, needswap);
    }
    else if (hdrtype == DMU_COMPOUNDSTREAM)
    {
        if (g_stream_to_stdout)
            parse_package_stream(&drr_begin, needswap);
        else
            parse_package(&drr_begin, needswap);
    }
    else
    {
        ZLOG(ZLOG_WARN, "Stream has unsupported feature %lx\n", featureflags);
    }

    dump_split_handler(g_spliter);
    free_split_handler(g_spliter);

    return 0;
}

