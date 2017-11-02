#ifndef _ZCUT_H_
#define _ZCUT_H_

typedef uint32_t boolean;

struct split_handler_s;
extern int g_wrfd;
extern int g_rdfd;
extern int g_dump_drr;
extern int g_stream_to_stdout;  /* defined in zcut.c */
extern char g_resume_snapshot_name[]; /* snapshot to resume */
extern int g_output_stage; /* switch of write or skip output */
extern boolean g_rename_snapshot;
extern struct split_handler_s *g_spliter;
extern zio_cksum_t g_overall_cksum;

#endif // _ZCUT_H_

