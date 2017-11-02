#ifndef _ZCUT_DRR_H_
#define _ZCUT_DRR_H_

#include "sys/zfs_ioctl.h"
#include "zcut.h"
#include "zcut_split.h"

#define IS_DRR_BEGIN(type) \
    ((type) == BSWAP_32(DRR_BEGIN) || (type) == DRR_BEGIN)

#define IS_DRR_END(type) \
    ((type) == BSWAP_32(DRR_END) || (type) == DRR_END)

#define DRR_SIZE        sizeof(dmu_replay_record_t)

int read_drr(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum, boolean needswap);
int read_pkg_begin_end(dmu_replay_record_t *ppkg_begin, dmu_replay_record_t *ppkg_end, void **pprop, boolean needswap);
int split_handler(dmu_replay_record_t *ppkg_begin, dmu_replay_record_t *ppkg_end, void *prop, dmu_replay_record_t *pcurr_begin, dmu_replay_record_t *pdrr, split_handler_t *spliter, zio_cksum_t *pzcksum, boolean needswap);
int drr_handler(dmu_replay_record_t *pdrr, split_handler_t *spliter, zio_cksum_t *pzcksum, boolean needswap);

#endif // _ZCUT_DRR_H_

