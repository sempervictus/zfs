#include "sys/zfs_ioctl.h"
#include "zfs_fletcher.h"

#include "zcut.h"
#include "zcut_dbg.h"
#include "zcut_drr.h"
#include "zcut_util.h"
#include "zcut_prop.h"

static int check_checksum(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum);
static int write_new_pkg_begin_end(dmu_replay_record_t *ppkg_begin, dmu_replay_record_t *ppkg_end, void *prop, dmu_replay_record_t *pcurr_begin, dmu_replay_record_t *pnew_begin, split_handler_t *spliter, boolean needswap);
static dmu_replay_record_t new_drr_end(dmu_replay_record_t *pdrr_begin, dmu_replay_record_t *pdrr_end, zio_cksum_t *pzcksum, boolean needswap);
static int copy_drr_object_payload(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum, boolean needswap);
static int copy_drr_write_payload(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum, boolean needswap);
static int write_drr(dmu_replay_record_t *pdrr, split_handler_t *spliter, zio_cksum_t *pzcksum, boolean needswap);
static int write_pkg_begin_end(dmu_replay_record_t *ppkg_begin, dmu_replay_record_t *ppkg_end, void *prop, uint32_t prop_len, split_handler_t *spliter, boolean needswap);
static int write_split_end(dmu_replay_record_t *pdrr_begin, dmu_replay_record_t *pdrr_end, zio_cksum_t *pzcksum, boolean needswap);
static int write_double_end(dmu_replay_record_t *pdrr_begin, dmu_replay_record_t *pdrr_end, zio_cksum_t *pzcksum, boolean needswap);

int
check_checksum(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum)
{
    struct drr_end *drre = &pdrr->drr_u.drr_end;

    if (!ZIO_CHECKSUM_EQUAL(drre->drr_checksum, *pzcksum))
    {
        ZLOG(ZLOG_MSG, "Expected checksum differs from checksum in stream\n"
             "END checksum = %lx/%lx/%lx/%lx\n"
             "Expected checksum[0-3] = %lx/%lx/%lx/%lx\n",
             drre->drr_checksum.zc_word[0], drre->drr_checksum.zc_word[1],
             drre->drr_checksum.zc_word[2], drre->drr_checksum.zc_word[3],
             pzcksum->zc_word[0], pzcksum->zc_word[1],
             pzcksum->zc_word[2], pzcksum->zc_word[3]);
        return -1;
    }
    else
    {
        ZLOG(ZLOG_DEBUG, "original END checksum = %lx/%lx/%lx/%lx\n",
             drre->drr_checksum.zc_word[0], drre->drr_checksum.zc_word[1],
             drre->drr_checksum.zc_word[2], drre->drr_checksum.zc_word[3]);
    }

    ZLOG(ZLOG_DEBUG, "Checksum pass\n");
    return 0;
}

/**
 * write_new_pkg_begin_end
 *
 *   create new properties and write the compound BEGIN, properties and
 *   END into next sub-stream.
 *
 * @param   ppkg_begin      [in]  compound BEGIN
 * @param   ppkg_end        [in]  compound END
 * @param   prop            [in]  properties' binary
 * @param   pcurr_begin     [in]  original drr BEGIN
 * @param   pnew_begin      [in]  split drr BEGIN
 * @param   spliter         [in]  split handler
 * @param   needswap        [in]  byte order needs to swap
 * @return  0 for success and other values for failure
 */
int
write_new_pkg_begin_end(
    dmu_replay_record_t *ppkg_begin,
    dmu_replay_record_t *ppkg_end,
    void *prop,
    dmu_replay_record_t *pcurr_begin,
    dmu_replay_record_t *pnew_begin,
    split_handler_t *spliter,
    boolean needswap)
{
    int err = 0;
    uint32_t prop_len = ppkg_begin->drr_payloadlen;
    void *new_prop = NULL;
    uint32_t new_prop_len = 0;

    if (needswap)
        prop_len = BSWAP_32(prop_len);

    // modify snaps' guid to match the above new BEGIN's toguid
    if (prop && prop_len)
    {
        err = new_property(pcurr_begin, pnew_begin, prop, prop_len,
                           needswap, &new_prop, &new_prop_len);

        if (err)
        {
            ZLOG(ZLOG_MSG, "Failed to create new property\n");
            return err;
        }
    }

    err = write_pkg_begin_end(ppkg_begin, ppkg_end, new_prop, new_prop_len,
                              spliter, needswap);

    if (err)
        ZLOG(ZLOG_MSG, "Failed to pkg BEGIN and pkg END\n");

    if (new_prop)
        free(new_prop);

    return err;
}

/**
 * new_drr_end
 *
 *   create a drr END for the given checksum or drr BEGIN's toguid
 *
 * @param   pdrr_begin  [in]  correspond drr BEGIN to this END
 * @param   pdrr_end    [in]  original drr END (may be NULL)
 * @param   pzcksum     [out] stream's checksum
 * @param   needswap    [in]  byte order needs to swap
 * @return  new drr END
 */
dmu_replay_record_t
new_drr_end(
    dmu_replay_record_t *pdrr_begin,
    dmu_replay_record_t *pdrr_end,
    zio_cksum_t *pzcksum,
    boolean needswap)
{
    dmu_replay_record_t drr = {0};
    struct drr_end *drre = &drr.drr_u.drr_end;

    if (pdrr_end)
        drr = *pdrr_end;

    if (needswap)
    {
        drr.drr_type = BSWAP_32(DRR_END);
        drre->drr_checksum.zc_word[0] = BSWAP_64(pzcksum->zc_word[0]);
        drre->drr_checksum.zc_word[1] = BSWAP_64(pzcksum->zc_word[1]);
        drre->drr_checksum.zc_word[2] = BSWAP_64(pzcksum->zc_word[2]);
        drre->drr_checksum.zc_word[3] = BSWAP_64(pzcksum->zc_word[3]);
        drre->drr_toguid = BSWAP_64(pdrr_begin->drr_u.drr_begin.drr_toguid);
    }
    else
    {
        drr.drr_type = DRR_END;
        drre->drr_checksum = *pzcksum;
        drre->drr_toguid = pdrr_begin->drr_u.drr_begin.drr_toguid;
    }

    return drr;
}

/**
 * copy_drr_object_payload
 *
 *   copy the content of drr OBJECT's payload from input stream to output stream
 *
 * @param   pdrr        [in]  drr OBJECT
 * @param   pzcksum     [out] stream's checksum
 * @param   needswap    [in]  byte order needs to swap
 * @return  negative value for error, 0 for no data and positive value for data length
 */
int
copy_drr_object_payload(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum, boolean needswap)
{
    struct drr_object *drro = &pdrr->drr_u.drr_object;
    uint32_t bonuslen = drro->drr_bonuslen;

    if (needswap)
        bonuslen = BSWAP_32(bonuslen);

    return copy_stream(g_rdfd, g_wrfd, P2ROUNDUP(bonuslen, 8), pzcksum, needswap);
}

/**
 * copy_drr_write_payload
 *
 *   copy the content of drr WRITE's payload from input stream to output stream
 *
 * @param   pdrr        [in]  drr WRITE
 * @param   pzcksum     [out] stream's checksum
 * @param   needswap    [in]  byte order needs to swap
 * @return  negative value for error, 0 for no data and positive value for data length
 */
int
copy_drr_write_payload(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum, boolean needswap)
{
    struct drr_write *drrw = &pdrr->drr_u.drr_write;
    uint64_t length = drrw->drr_length;

    if (needswap)
        length = BSWAP_64(length);

    return copy_stream(g_rdfd, g_wrfd, length, pzcksum, needswap);
}

/**
 * read_drr
 *
 *   Read a dmu_replay_record_t from g_rdfd
 *
 * @param   pdrr        [out] drr buffer
 * @param   pzcksum     [out] stream's ckecksum
 * @param   needswap    [in]  byte order needs to swap
 * @return  negative values for failure, 0 for no data and positive values for data length
 */
int
read_drr(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum, boolean needswap)
{
    int ret = read_stream(g_rdfd, pdrr, DRR_SIZE);
    char *p = NULL;

    if (ret <= 0)
        return ret;

    if (!IS_DRR_END(pdrr->drr_type))
    {
        if (g_rename_snapshot)
        {
            if (IS_DRR_BEGIN(pdrr->drr_type))
            {
                if ((p = strchr(pdrr->drr_u.drr_begin.drr_toname, '@')) != NULL)
                {
                    new_alias_name(p + 1);
                }
            }
        }

        if (needswap)
            fletcher_4_incremental_byteswap(pdrr, DRR_SIZE, pzcksum);
        else
            fletcher_4_incremental_native(pdrr, DRR_SIZE, pzcksum);
    }

    return ret;
}

/**
 * write_drr
 *
 *   Write a dmu_replay_record_t to g_wrfd
 *
 * @param   pdrr        [in]  drr to be written
 * @param   spliter     [in]  split handler
 * @param   pzcksum     [out] stream's checksum
 * @param   needswap    [in]  byte order needs to swap
 * @return  0 for success and other values for failure
 */
int
write_drr(
    dmu_replay_record_t *pdrr,
    split_handler_t *spliter,
    zio_cksum_t *pzcksum,
    boolean needswap)
{
    int ret = 0;
    char tmp_buf[256];
    char *p = NULL;

    if (!IS_DRR_END(pdrr->drr_type))
    {
        if (g_rename_snapshot)
        {
            if (IS_DRR_BEGIN(pdrr->drr_type))
            {
                if ((p = strchr(pdrr->drr_u.drr_begin.drr_toname, '@')) != NULL)
                {
                    strcpy(tmp_buf, p + 1);
                    strcpy(p + 1, new_alias_name(tmp_buf));
                }
            }
        }

        if (needswap)
            fletcher_4_incremental_byteswap(pdrr, DRR_SIZE, pzcksum);
        else
            fletcher_4_incremental_native(pdrr, DRR_SIZE, pzcksum);
    }

    ret = write_stream(g_wrfd, pdrr, DRR_SIZE);
    if (ret < 0)
        return -ret;

    if (g_dump_drr)
        dump_drr(pdrr, pzcksum, needswap);

    split_counter(spliter, DRR_SIZE, 1);

    return 0;
}

/*
 * read_pkg_begin_to_end
 *
 *   read compound stream's first header (includes properties)
 *
 * @param   ppkg_begin  [in]  compound stream's first BEGIN
 * @param   ppkg_end    [out] return compound stream's first END
 * @param   pprop       [out] return compound stream's properties if there is,
 *                            otherwise return NULL
 * @param   needswap    [in]  stream's byte order needs to swap
 *
 * @return  0 for success and other values for failure
 */
int
read_pkg_begin_end(
    /**
     * Try to satisfy const correctness. Parameter ppkg_begin should
     * be declared as dmu_reply_record_t const *ppkg_begin.
     */
    dmu_replay_record_t *ppkg_begin,
    dmu_replay_record_t *ppkg_end,
    void **pprop,
    boolean needswap)
{
    int ret = 0;
    zio_cksum_t zcksum = {{0}};
    uint32_t prop_len = ppkg_begin->drr_payloadlen;
    void *prop = NULL;
    dmu_replay_record_t pkg_end;

    if (needswap)
    {
        prop_len = BSWAP_32(prop_len);
        fletcher_4_incremental_byteswap(ppkg_begin, DRR_SIZE, &zcksum);
    }
    else
        fletcher_4_incremental_native(ppkg_begin, DRR_SIZE, &zcksum);

    if (prop_len != 0)
    {
        prop = malloc(prop_len);
        if (NULL == prop)
        {
            ZLOG(ZLOG_WARN, "Failed to allocate memory for properties\n");
            return ENOMEM;
        }

        ret = read_stream(g_rdfd, prop, prop_len);
        if (ret < 0)
            return -ret;

        if (needswap)
            fletcher_4_incremental_byteswap(prop, prop_len, &zcksum);
        else
            fletcher_4_incremental_native(prop, prop_len, &zcksum);
    }

    bzero(&pkg_end, DRR_SIZE);
    ret = read_drr(&pkg_end, &g_overall_cksum, needswap);
    if (ret < 0)
        return -ret;

    check_checksum(&pkg_end, &zcksum);

    *pprop = prop;
    *ppkg_end = pkg_end;

    return 0;
}

/**
 * write_pkg_begin_end
 *
 *  write stream's compound BEGIN, properties and END to split sub-stream
 *
 * @param   ppkg_begin  [in]  the first BEGIN of the compound stream
 * @param   ppkg_end    [in]  the first END of the compound stream
 * @param   prop        [in]  the properties in the compound stream
 * @param   prop_len    [in]  the length of the properties
 * @param   spliter     [in]  split handler
 * @param   needswap    [in]  byte order needs to swap
 * @return  0 for success and other values for failure
 */
int
write_pkg_begin_end(
    dmu_replay_record_t *ppkg_begin,
    dmu_replay_record_t *ppkg_end,
    void *prop,
    uint32_t prop_len,
    split_handler_t *spliter,
    boolean needswap)
{
    int ret = 0;
    zio_cksum_t zcksum = {{0}};
    dmu_replay_record_t pkg_begin = *ppkg_begin;

    // write BEGIN
    pkg_begin.drr_payloadlen = (needswap)? BSWAP_32(prop_len) : prop_len;
    ret = write_drr(&pkg_begin, spliter, &zcksum, needswap);
    if (ret)
    {
        ZLOG(ZLOG_MSG, "Failed to write pkg begin\n");
        return ret;
    }

    // properties
    if (prop_len && prop)
    {
        ret = write_stream(g_wrfd, prop, prop_len);
        if (ret < 0)
        {
            ZLOG(ZLOG_MSG, "Failed to write properties\n");
            return -ret;
        }

        if (g_dump_drr)
            dump_prop(prop, prop_len);

        split_counter(spliter, ret, 0);

        if (needswap)
            fletcher_4_incremental_byteswap(prop, prop_len, &zcksum);
        else
            fletcher_4_incremental_native(prop, prop_len, &zcksum);
    }

    // write END
    ret = write_split_end(ppkg_begin, ppkg_end, &zcksum, needswap);
    if (ret)
    {
        ZLOG(ZLOG_MSG, "Failed to write pkg end\n");
        return ret;
    }

    split_counter(spliter, DRR_SIZE, 1);

    return 0;
}

/**
 * write_split_end
 *
 *   write a drr END for the current sub-stream
 *
 * @param   pdrr_begin  [in]  drr BEGIN to the current sub-stream
 * @param   pdrr_end    [in]  read drr END (may be NULL)
 * @param   pzcksum     [in]  stream's checksum
 * @param   needswap    [in]  byte order needs to swap
 * @return  0 for success and other values for failure
 */
int
write_split_end(
    dmu_replay_record_t *pdrr_begin,
    dmu_replay_record_t *pdrr_end,
    zio_cksum_t *pzcksum,
    boolean needswap)
{
    int ret = 0;
    dmu_replay_record_t new_end;

    new_end = new_drr_end(pdrr_begin, pdrr_end, pzcksum, needswap);

    if (g_dump_drr)
        dump_end(&new_end, pzcksum);

    ret = write_stream(g_wrfd, &new_end, DRR_SIZE);

    return (ret < 0)? -ret : 0;
}

/**
 * write_double_end
 *
 *   write a drr END and an ended drr END for the current compound sub-stream
 *
 * @param   pdrr_begin  [in]  drr BEGIN to the current sub-stream
 * @param   pdrr_end    [in]  read drr END (may be NULL)
 * @param   pzcksum     [in]  stream's checksum
 * @param   needswap    [in]  byte order needs to swap
 * @return  0 for success and other values for failure
 */
int
write_double_end(
    dmu_replay_record_t *pdrr_begin,
    dmu_replay_record_t *pdrr_end,
    zio_cksum_t *pzcksum,
    boolean needswap)
{
    int ret = 0;
    dmu_replay_record_t new_end;
    zio_cksum_t zcksum = {{0}};

    ret = write_split_end(pdrr_begin, pdrr_end, pzcksum, needswap);
    if (ret)
        return ret;

    new_end = new_drr_end(pdrr_begin, NULL, &zcksum, needswap);

    if (g_dump_drr)
        dump_end(&new_end, &zcksum);

    ret = write_stream(g_wrfd, &new_end, DRR_SIZE);

    return (ret < 0)? -ret : 0;
}

/**
 * split_handler
 *
 *   write drr END for the current sub-stream and then
 *   write an ended sub-stream
 *
 * @param   ppkg_begin  [in]  compound BEGIN (may be NULL)
 * @param   ppkg_end    [in]  compound END (may be NULL)
 * @parm    prop        [in]  properties (may be NULL)
 * @param   pcurr_begin [in]  drr BEGIN to the current sub-stream
 * @param   pdrr        [in]  next read drr
 * @param   spliter     [in]  stream's split handler
 * @param   pzcksum     [in]  stream's checksum
 * @param   needswap    [in]  byte order needs to swap
 * @return  
 */
int
split_handler(
    dmu_replay_record_t *ppkg_begin,
    dmu_replay_record_t *ppkg_end,
    void *prop,
    dmu_replay_record_t *pcurr_begin,
    dmu_replay_record_t *pdrr,
    split_handler_t *spliter,
    zio_cksum_t *pzcksum,
    boolean needswap)
{
    int ret = 0;
    dmu_replay_record_t new_begin;
    uint32_t drr_type = pdrr->drr_type;
    char *snapshot_name = NULL;

    // split END
    if (IS_DRR_BEGIN(drr_type))
    {
    }
    else if (IS_DRR_END(drr_type))
    {
        if (is_first_split(spliter) && ppkg_begin)
            ret = write_double_end(pcurr_begin, pdrr, pzcksum, needswap);
        else
            ret = write_split_end(pcurr_begin, pdrr, pzcksum, needswap);

        check_checksum(pdrr, &g_overall_cksum);
        bzero(&g_overall_cksum, sizeof(zio_cksum_t));
        bzero(pzcksum, sizeof(zio_cksum_t));
        split_counter(spliter, DRR_SIZE, 1);
        if (ppkg_end != NULL && IS_DRR_BEGIN(ppkg_end->drr_type))
            return ret;
    }
    else
    {
        if (is_first_split(spliter) && ppkg_begin)
            ret = write_double_end(pcurr_begin, NULL, pzcksum, needswap);
        else
            ret = write_split_end(pcurr_begin, NULL, pzcksum, needswap);

        bzero(pzcksum, sizeof(zio_cksum_t));
        split_counter(spliter, DRR_SIZE, 1);
    }

    if (ret)
    {
        ZLOG(ZLOG_MSG, "Failed to write END\n");
        return ret;
    }

    // split BEGIN
    new_begin = next_split_begin(pcurr_begin, spliter, needswap, IS_DRR_END(drr_type));

    if ((IS_DRR_BEGIN(drr_type) || IS_DRR_END(drr_type)) && ppkg_begin)
    {
        ret = write_new_pkg_begin_end(ppkg_begin, ppkg_end, prop, pcurr_begin, &new_begin, spliter, needswap);
        if (ret)
            return ret;
    }

    if (strlen(g_resume_snapshot_name) > 0) /* intermedium snapshot is given */
    {
        snapshot_name = strchr(new_begin.drr_u.drr_begin.drr_toname, '@');
        if (snapshot_name != NULL && strcmp(g_resume_snapshot_name, (snapshot_name + 1)) == 0)
        {
            /* this intermedium snapshot is replicated. Next snapshot is the one to start resume */
            g_output_stage = -1;
        }
        else if (g_output_stage == 1)
        {
            /* write beginning header is done, close write */
            g_output_stage = 0;
        }
        else if (g_output_stage == -1)
        {
            /* it's the snapshot to resume now */
            g_output_stage = 2;
        }
    }

    ret = write_drr(&new_begin, spliter, pzcksum, needswap);
    if (ret)
    {
        ZLOG(ZLOG_MSG, "Failed to write BEGIN\n");
        return ret;
    }

    ZLOG(ZLOG_DEBUG, "new BEGIN checksum = %lx/%lx/%lx/%lx\n",
         pzcksum->zc_word[0], pzcksum->zc_word[1],
         pzcksum->zc_word[2], pzcksum->zc_word[3]);

    if (IS_DRR_END(drr_type))
    {
        if (ppkg_begin)
        {
            ret = write_double_end(&new_begin, NULL, pzcksum, needswap);
            if (!ret && g_stream_to_stdout)
            {
                /* another END is required if the last DRR contains nvlist */
                bzero(pzcksum, sizeof(zio_cksum_t));
                ret = write_split_end(&new_begin, NULL, pzcksum, needswap);
            }
        }
        else
        {
            ret = write_split_end(&new_begin, NULL, pzcksum, needswap);
        }

        if (ret)
            ZLOG(ZLOG_MSG, "Failed to write END\n");

        bzero(pzcksum, sizeof(zio_cksum_t));
    }

    return ret;
}

int
drr_handler(
    dmu_replay_record_t *pdrr,
    split_handler_t *spliter,
    zio_cksum_t *pzcksum,
    boolean needswap)
{
    int ret = 0;
    uint32_t drr_type = (needswap)? BSWAP_32(pdrr->drr_type) : pdrr->drr_type;

    ret = write_drr(pdrr, spliter, pzcksum, needswap);
    if (ret)
        return ret;

    switch (drr_type)
    {
        case DRR_OBJECT:
            ret = copy_drr_object_payload(pdrr, pzcksum, needswap);
            if (ret > 0)
                split_counter(spliter, ret, 0);
            break;
        case DRR_WRITE:
            ret = copy_drr_write_payload(pdrr, pzcksum, needswap);
            if (ret > 0)
                split_counter(spliter, ret, 0);
            break;
        case DRR_BEGIN:
        case DRR_END:
        case DRR_FREEOBJECTS:
        case DRR_WRITE_BYREF:
        case DRR_FREE:
        case DRR_SPILL:
            break;
    }

    return (ret < 0)? -ret : 0;
}

