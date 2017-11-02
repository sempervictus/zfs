#ifndef _ZCUT_PROP_H_
#define _ZCUT_PROP_H_

#include "zcut.h"

int new_property(dmu_replay_record_t *pcurr_begin, dmu_replay_record_t *pnew_begin, void *prop, uint32_t prop_len, boolean needswap, void **new_prop, uint32_t *new_prop_len);

#endif // _ZCUT_PROP_H_

