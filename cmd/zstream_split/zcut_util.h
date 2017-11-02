#ifndef _ZCUT_UTIL_H_
#define _ZCUT_UTIL_H_

#include "sys/zfs_ioctl.h"
#include "zcut.h"


/***************************************************
 *
 * drr dump for debug
 *
 ***************************************************/
void dump_prop(char *prop, uint64_t len);
void dump_begin(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum);
void dump_object(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum);
void dump_freeobjects(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum);
void dump_write(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum);
void dump_write_byref(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum);
void dump_free(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum);
void dump_spill(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum);
void dump_end(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum);
void dump_drr(dmu_replay_record_t *pdrr, zio_cksum_t *pzcksum, boolean_t needswap);
void byteswap(dmu_replay_record_t *pdrr);

/***************************************************
 *
 * read/write stream
 *
 ***************************************************/
int read_stream(int fd, void *buf, int len);
int write_stream(int fd, void *buf, int len);
int copy_stream(int infd, int outfd, int len, zio_cksum_t *pzcksum, boolean needswap);

/***************************************************
 *
 * rename final snapshot
 *
 ***************************************************/
char * new_alias_name(char *origin_name);

#endif // _ZCUT_UTIL_H_

