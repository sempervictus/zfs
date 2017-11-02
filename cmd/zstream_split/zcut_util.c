#include "sys/zfs_ioctl.h"
#include "zfs_fletcher.h"
#include "sys/nvpair.h"
#include "libnvpair.h"
#include <uuid/uuid.h>

#include "zcut.h"
#include "zcut_util.h"
#include "zcut_dbg.h"

#define READ_BUF_LEN            (1 << 20)   // 1 MB

typedef struct drr_func_s
{
    uint32_t drr_type;
    void (*drr_dump)(dmu_replay_record_t *, zio_cksum_t *);
} drr_func_t;

drr_func_t drr_hdl_tbl[] =
{
    {DRR_BEGIN,         dump_begin},
    {DRR_OBJECT,        dump_object},
    {DRR_FREEOBJECTS,   dump_freeobjects},
    {DRR_WRITE,         dump_write},
    {DRR_WRITE_BYREF,   dump_write_byref},
    {DRR_FREE,          dump_free},
    {DRR_SPILL,         dump_spill},
    {DRR_END,           dump_end},
};


/***************************************************
 *
 * drr dump for debug
 *
 ***************************************************/
void
dump_prop(char *prop, uint64_t len)
{
    nvlist_t *nvl = NULL;
    int err = nvlist_unpack(prop, len, &nvl, 0);

    if (err)
        ZLOG(ZLOG_MSG, "Failed to unpack nvlist, err = %u, nvl = %p\n", err, nvl);

    nvlist_print(stderr, nvl);
    nvlist_free(nvl);
}

void
dump_begin(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum)
{
    struct drr_begin *drrb = &pdrr->drr_u.drr_begin;
    int hdrtype = DMU_GET_STREAM_HDRTYPE(drrb->drr_versioninfo);
    uint64_t featureflags = DMU_GET_FEATUREFLAGS(drrb->drr_versioninfo);

    fprintf(stderr, "BEGIN record\n");
    fprintf(stderr, "\thdrtype = %u\n", hdrtype);
    fprintf(stderr, "\tfeatures = %lu\n", featureflags);
    fprintf(stderr, "\tmagic = %lx\n", drrb->drr_magic);
    fprintf(stderr, "\tcreation time = %lx\n", drrb->drr_creation_time);
    fprintf(stderr, "\ttype = %u\n", drrb->drr_type);
    fprintf(stderr, "\tflags = 0x%x\n", drrb->drr_flags);
    fprintf(stderr, "\ttoguid = %lx\n", drrb->drr_toguid);
    fprintf(stderr, "\tfromguid = %lx\n", drrb->drr_fromguid);
    fprintf(stderr, "\ttoname = %s\n", drrb->drr_toname);
    fprintf(stderr, "\n");
}

void
dump_object(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum)
{
    struct drr_object *drro = &pdrr->drr_u.drr_object;

    fprintf(stderr, "OBJECT object = %lu type = %u bonustype = %u "
            "blksz = %u bonuslen = %u\n",
            drro->drr_object, drro->drr_type, drro->drr_bonustype,
            drro->drr_blksz, drro->drr_bonuslen);
}

void
dump_freeobjects(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum)
{
    struct drr_freeobjects *drrfo = &pdrr->drr_u.drr_freeobjects;

    fprintf(stderr, "FREEOBJECTS firstobj = %lu numobjs = %lu\n",
            drrfo->drr_firstobj, drrfo->drr_numobjs);
}

void
dump_write(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum)
{
    struct drr_write *drrw = &pdrr->drr_u.drr_write;

    fprintf(stderr, "WRITE object = %lu type = %u checksum type = %u\n"
            "offset = %lu length = %lu props = %lx\n",
            drrw->drr_object, drrw->drr_type, drrw->drr_checksumtype,
            drrw->drr_offset, drrw->drr_length, drrw->drr_key.ddk_prop);
}

void
dump_write_byref(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum)
{
    struct drr_write_byref *drrwb = &pdrr->drr_u.drr_write_byref;

    fprintf(stderr, "WRITE_BYREF object = %lu checksum type = %u props = %lx\n"
            "offset = %lu length = %lu\n"
            "toguid = %lx refguid = %lx\n"
            "refobject = %lu refoffset = %lu\n",
            drrwb->drr_object, drrwb->drr_checksumtype,
            drrwb->drr_key.ddk_prop, drrwb->drr_offset,
            drrwb->drr_length, drrwb->drr_toguid,
            drrwb->drr_refguid, drrwb->drr_refobject,
            drrwb->drr_refoffset);
}

void
dump_free(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum)
{
    struct drr_free *drrf = &pdrr->drr_u.drr_free;

    fprintf(stderr, "FREE object = %lu offset = %lu length = %ld\n",
            drrf->drr_object, drrf->drr_offset, drrf->drr_length);
}

void
dump_spill(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum)
{
    struct drr_spill *drrs = &pdrr->drr_u.drr_spill;

    fprintf(stderr, "SPILL object = %lu length = %lu\n",
            drrs->drr_object, drrs->drr_length);
}

void
dump_end(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum)
{
    struct drr_end *drre = &pdrr->drr_u.drr_end;

    fprintf(stderr, "END checksum = %lx/%lx/%lx/%lx\n",
            drre->drr_checksum.zc_word[0],
            drre->drr_checksum.zc_word[1],
            drre->drr_checksum.zc_word[2],
            drre->drr_checksum.zc_word[3]);
}

void
dump_drr(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum, boolean needswap)
{
    int i = 0;
    dmu_replay_record_t drr = *pdrr;
    uint32_t drr_type = pdrr->drr_type;

    if (needswap)
        drr_type = BSWAP_32(drr_type);

    for (; i < DRR_NUMTYPES; ++i)
    {
        if (drr_hdl_tbl[i].drr_type == drr_type)
        {
            if (needswap)
                byteswap(&drr);

            drr_hdl_tbl[i].drr_dump(&drr, pzcksum);
            break;
        }
    }
}

void
byteswap(dmu_replay_record_t *pdrr)
{
    pdrr->drr_type = BSWAP_32(pdrr->drr_type);
    pdrr->drr_payloadlen = BSWAP_32(pdrr->drr_payloadlen);

#define DO64(X) (pdrr->drr_u.X = BSWAP_64(pdrr->drr_u.X))
#define DO32(X) (pdrr->drr_u.X = BSWAP_32(pdrr->drr_u.X))
    switch (pdrr->drr_type)
    {
    case DRR_BEGIN:
        DO64(drr_begin.drr_magic);
        DO64(drr_begin.drr_versioninfo);
        DO64(drr_begin.drr_creation_time);
        DO32(drr_begin.drr_type);
        DO32(drr_begin.drr_flags);
        DO64(drr_begin.drr_toguid);
        DO64(drr_begin.drr_fromguid);
        break;
    case DRR_OBJECT:
        DO64(drr_object.drr_object);
        DO32(drr_object.drr_type);
        DO32(drr_object.drr_bonustype);
        DO32(drr_object.drr_blksz);
        DO32(drr_object.drr_bonuslen);
        DO64(drr_object.drr_toguid);
        break;
    case DRR_FREEOBJECTS:
        DO64(drr_freeobjects.drr_firstobj);
        DO64(drr_freeobjects.drr_numobjs);
        DO64(drr_freeobjects.drr_toguid);
        break;
    case DRR_WRITE:
        DO64(drr_write.drr_object);
        DO32(drr_write.drr_type);
        DO32(drr_write.drr_pad);
        DO64(drr_write.drr_offset);
        DO64(drr_write.drr_length);
        DO64(drr_write.drr_toguid);
        DO64(drr_write.drr_key.ddk_cksum.zc_word[0]);
        DO64(drr_write.drr_key.ddk_cksum.zc_word[1]);
        DO64(drr_write.drr_key.ddk_cksum.zc_word[2]);
        DO64(drr_write.drr_key.ddk_cksum.zc_word[3]);
        DO64(drr_write.drr_key.ddk_prop);
        break;
    case DRR_WRITE_BYREF:
        DO64(drr_write_byref.drr_object);
        DO64(drr_write_byref.drr_offset);
        DO64(drr_write_byref.drr_length);
        DO64(drr_write_byref.drr_toguid);
        DO64(drr_write_byref.drr_refguid);
        DO64(drr_write_byref.drr_refobject);
        DO64(drr_write_byref.drr_refoffset);
        DO64(drr_write_byref.drr_key.ddk_cksum.zc_word[0]);
        DO64(drr_write_byref.drr_key.ddk_cksum.zc_word[1]);
        DO64(drr_write_byref.drr_key.ddk_cksum.zc_word[2]);
        DO64(drr_write_byref.drr_key.ddk_cksum.zc_word[3]);
        DO64(drr_write_byref.drr_key.ddk_prop);
        break;
    case DRR_FREE:
        DO64(drr_free.drr_object);
        DO64(drr_free.drr_offset);
        DO64(drr_free.drr_length);
        DO64(drr_free.drr_toguid);
        break;
    case DRR_SPILL:
        DO64(drr_spill.drr_object);
        DO64(drr_spill.drr_length);
        DO64(drr_spill.drr_toguid);
        break;
    case DRR_END:
        DO64(drr_end.drr_checksum.zc_word[0]);
        DO64(drr_end.drr_checksum.zc_word[1]);
        DO64(drr_end.drr_checksum.zc_word[2]);
        DO64(drr_end.drr_checksum.zc_word[3]);
        DO64(drr_end.drr_toguid);
        break;
    default:
        break;
    }
#undef DO64
#undef DO32
}


/***************************************************
 *
 * read/write stream
 *
 ***************************************************/
/**
 * read_stream
 *
 *   read data from the given file descriptor
 *
 * @param   fd      [in]  file descriptor
 * @param   buf     [out] data buffer
 * @param   len     [in]  read length
 * @return  negative value for error, 0 for no data and positive value for read length
 */
int
read_stream(int fd, void *buf, int len)
{
    char *cp = buf;
    int resi = len;
    int rv = 0;

    do
    {
        rv = read(fd, cp, resi);
        cp += rv;
        resi -= rv;
    } while (rv > 0);

    if (rv < 0)
    {
        ZLOG(ZLOG_MSG, "Failed to read from stream. erron = %u\n", errno);
        return -errno;
    }

    return len - resi;
}

/**
 * write_stream
 *
 *   write data to the given file descriptor
 *
 * @param   fd      [in]  file descriptor
 * @param   buf     [in]  data buffer
 * @param   len     [in]  write length
 * @return  negative value for error, 0 for no data and positive value for write length
 */
int
write_stream(int fd, void *buf, int len)
{
    int rv;

    if (g_stream_to_stdout && g_output_stage <= 0)
    {
        return len;
    }

    rv = write(fd, buf, len);
    if (rv < 0)
    {
        ZLOG(ZLOG_MSG, "Failed to write to file. errno = %u\n", errno);
        return -errno;
    }
    
    return rv;
}

/**
 * copy_stream
 *
 *   read data from infd and then write to outfd directly
 *
 * @param   infd        [in]  file descriptor for read
 * @param   outfd       [in]  file descriptor for write
 * @param   len         [in]  data length
 * @param   pzcksum     [out] stream's checksum
 * @param   needswap    [in]  data need to swap
 * @return  negative value for error, 0 for no data and positive value for data length
 */
int
copy_stream(int infd, int outfd, int len, zio_cksum_t *pzcksum, boolean needswap)
{
    int ret = 0;
    char *buf = NULL;
    int total_len = len;
    int resi = len;

    buf = malloc(READ_BUF_LEN);

    if (NULL == buf)
    {
        ZLOG(ZLOG_WARN, "Failed to allocate memory for read buffer\n");
        return -ENOMEM;
    }

    while (resi)
    {
        len = (resi > READ_BUF_LEN)? READ_BUF_LEN : resi;
        ret = read_stream(infd, buf, len);
        if (ret <= 0)
            break;

        ret = write_stream(outfd, buf, ret);
        if (ret <= 0)
            break;

        if (needswap)
        {
            fletcher_4_incremental_byteswap(buf, ret, &g_overall_cksum);
            fletcher_4_incremental_byteswap(buf, ret, pzcksum);
        }
        else
        {
            fletcher_4_incremental_native(buf, ret, &g_overall_cksum);
            fletcher_4_incremental_native(buf, ret, pzcksum);
        }

        resi -= ret;
    }

    free(buf);

    return (ret < 0)? ret : total_len - resi;
}

/**
 * uuid_uuid4_hex
 *
 *   generate a new 32-character uuid4 string
 *
 * @param   origin_uuid     [in] origin uuid
 * @return  uuid4 string
 */
static char *
uuid_uuid4_hex(void)
{
    static char uuid_buf[36] = {0};
    uuid_t new_uuid;
    int i;

    uuid_generate(new_uuid);
    for (i = 0; i < sizeof(uuid_t); i++)
    {
        sprintf(&uuid_buf[i*2], "%02x", *((unsigned char *)new_uuid + i));
    }
    uuid_buf[sizeof(uuid_t)*2] = '\0';
    return uuid_buf;
}

/**
 * new_alias_name
 *
 *   generate and cahce the first name that should be the end snapshot name.
 *
 * @param   origin_name     [in] original name
 * @return  new or cahced uuid4 name
 */
char *
new_alias_name(char *origin_name)
{
    static char cached_origin_name[36] = {0};
    static char cached_new_name[36] = {0};

    if (!g_rename_snapshot)
    {
        return origin_name;
    }
    if (strlen(cached_origin_name) > 0 && strcmp(cached_origin_name, origin_name) == 0)
        return cached_new_name;
    if (strlen(cached_origin_name) == 0)
    {
        strcpy(cached_origin_name, origin_name);
        strcpy(cached_new_name, uuid_uuid4_hex());
        return cached_new_name;
    }
    return origin_name;
}
