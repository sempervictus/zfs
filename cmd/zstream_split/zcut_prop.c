#include "sys/zfs_ioctl.h"
#include "sys/nvpair.h"
#include "zcut_prop.h"
#include "zcut_dbg.h"
#include "zcut_util.h"
#include "zcut.h"


typedef int (*fp_nvlist_handler)(nvlist_t *src_nvl, nvpair_t *nvp, void *user_data, nvlist_t *dest_nvl);

typedef struct prop_info_s
{
    uint64_t ori_toguid;
    uint64_t new_toguid;
    char *ori_toname;
    char *new_toname;
} prop_info_t;

static int begin_prop_hdlr(nvlist_t *src_nvl, nvpair_t *nvp, void *user_data, nvlist_t *dest_nvl);
static int fss_hdlr(nvlist_t *src_nvl, nvpair_t *nvp, void *user_data, nvlist_t *dest_nvl);
static int vol_hdlr(nvlist_t *src_nvl, nvpair_t *nvp, void *user_data, nvlist_t *dest_nvl);
static int snaps_hdlr(nvlist_t *src_nvl, nvpair_t *nvp, void *user_data, nvlist_t *dest_nvl);
static int snapprops_hdlr(nvlist_t *src_nvl, nvpair_t *nvp, void *user_data, nvlist_t *dest_nvl);
static int nvlist_duplicator(nvlist_t *src_nvl, void *user_data, fp_nvlist_handler nvlist_hdlr, nvlist_t **dest_nvl);


int
begin_prop_hdlr(nvlist_t *src_nvl, nvpair_t *nvp, void *user_data, nvlist_t *dest_nvl)
{
    int err = 0;
    char *name = nvpair_name(nvp);
    dmu_replay_record_t *drr_begin = (dmu_replay_record_t *)user_data;
    struct drr_begin *drrb = &drr_begin->drr_u.drr_begin;

    if (0 == strcmp(name, "toguid"))
        err = nvlist_add_uint64(dest_nvl, name, drrb->drr_toguid);
    else if (0 == strcmp(name, "fromsnap") && g_rename_snapshot)
        err = nvlist_add_string(dest_nvl, name, "");
    else if (0 == strcmp(name, "tosnap"))
        err = nvlist_add_string(dest_nvl, name, new_alias_name(NVP_VALUE(nvp)));
    else if (0 == strcmp(name, "fss"))
    {
        nvlist_t *fss_nvl = NULL;
        nvlist_t *new_fss = NULL;

        err = nvpair_value_nvlist(nvp, &fss_nvl);
        if (err)
        {
            ZLOG(ZLOG_MSG, "Couldn't get fss' name-value list\n");
            return err;
        }

        err = nvlist_duplicator(fss_nvl, user_data, fss_hdlr, &new_fss);
        if (err)
            return err;

        err = nvlist_add_nvlist(dest_nvl, name, new_fss);
        if (err)
            nvlist_free(new_fss);
    }
    else
        err = nvlist_add_nvpair(dest_nvl, nvp);

    if (err)
        ZLOG(ZLOG_MSG, "Failed to add %s into begin properties\n", name);

    return err;
}

int
fss_hdlr(nvlist_t *src_nvl, nvpair_t *nvp, void *user_data, nvlist_t *dest_nvl)
{
    int err = 0;
    nvlist_t *vol_nvl = NULL;
    nvlist_t *new_vol = NULL;

    err = nvpair_value_nvlist(nvp, &vol_nvl);
    if (err)
    {
        ZLOG(ZLOG_MSG, "Couldn't get volume's name-value list\n");
        return err;
    }

    err = nvlist_duplicator(vol_nvl, user_data, vol_hdlr, &new_vol);
    if (err)
        return err;

    err = nvlist_add_nvlist(dest_nvl, nvpair_name(nvp), new_vol);
    if (err)
    {
        ZLOG(ZLOG_MSG, "Failed to add %s into fss\n", nvpair_name(nvp));
        nvlist_free(new_vol);
    }

    return err;
}

int
vol_hdlr(nvlist_t *src_nvl, nvpair_t *nvp, void *user_data, nvlist_t *dest_nvl)
{
    int err = 0;
    char *name = nvpair_name(nvp);

    if (0 == strcmp(name, "snaps"))
    {
        nvlist_t *snaps_nvl = NULL;
        nvlist_t *new_snaps = NULL;

        err = nvpair_value_nvlist(nvp, &snaps_nvl);
        if (err)
        {
            ZLOG(ZLOG_MSG, "Couldn't get snaps' name-value list\n");
            return err;
        }

        err = nvlist_duplicator(snaps_nvl, user_data, snaps_hdlr, &new_snaps);
        if (err)
            return err;

        err = nvlist_add_nvlist(dest_nvl, name, new_snaps);
        if (err)
            nvlist_free(new_snaps);
    }
    else if (0 == strcmp(name, "snapprops"))
    {
        nvlist_t *snapprops_nvl = NULL;
        nvlist_t *new_snapprops = NULL;

        err = nvpair_value_nvlist(nvp, &snapprops_nvl);
        if (err)
        {
            ZLOG(ZLOG_MSG, "Couldn't get snapprops' name-value list\n");
            return err;
        }

        err = nvlist_duplicator(snapprops_nvl, user_data, snapprops_hdlr, &new_snapprops);
        if (err)
            return err;

        err = nvlist_add_nvlist(dest_nvl, name, new_snapprops);
        if (err)
            nvlist_free(new_snapprops);
    }
    else
        err = nvlist_add_nvpair(dest_nvl, nvp);

    if (err)
        ZLOG(ZLOG_MSG, "Failed to add %s into fss volume\n", name);

    return err;
}

int
snaps_hdlr(nvlist_t *src_nvl, nvpair_t *nvp, void *user_data, nvlist_t *dest_nvl)
{
    int err = 0;
    prop_info_t prop_info = *(prop_info_t *)user_data;
    uint64_t thisguid;

    err = nvpair_value_uint64(nvp, &thisguid);
    if (err)
    {
        ZLOG(ZLOG_MSG, "Couldn't get snaps %s's toguid\n", nvpair_name(nvp));
        return err;
    }

    if (g_rename_snapshot)
    {
        if (strcmp(nvpair_name(nvp), new_alias_name(nvpair_name(nvp))) != 0)
            err = nvlist_add_uint64(dest_nvl, new_alias_name(nvpair_name(nvp)), thisguid);
    }
    else
    {
        if (prop_info.ori_toguid == thisguid)
            err = nvlist_add_uint64(dest_nvl, prop_info.new_toname, prop_info.new_toguid);
        else
            err = nvlist_add_nvpair(dest_nvl, nvp);
    }

    if (err)
        ZLOG(ZLOG_MSG, "Failed to add %s into snaps\n", nvpair_name(nvp));

    return err;
}

int
snapprops_hdlr(nvlist_t *src_nvl, nvpair_t *nvp, void *user_data, nvlist_t *dest_nvl)
{
    int err = 0;
    prop_info_t prop_info = *(prop_info_t *)user_data;
    char *name = nvpair_name(nvp);

    if ((0 != strcmp(name, new_alias_name(name)) && g_rename_snapshot) ||
        (0 == strcmp(name, prop_info.ori_toname) && !g_rename_snapshot))
    {
        nvlist_t *nvl = NULL;

        err = nvpair_value_nvlist(nvp, &nvl);
        if (err)
        {
            ZLOG(ZLOG_MSG, "Couldn't get snapprop %s's nvlist\n", name);
            return err;
        }

        err = nvlist_add_nvlist(dest_nvl, new_alias_name(prop_info.new_toname), nvl);
    }
    else if (!g_rename_snapshot)
    {
        err = nvlist_add_nvpair(dest_nvl, nvp);
    }

    if (err)
        ZLOG(ZLOG_MSG, "Failed to add %s into snapprops\n", name);

    return err;
}

int
nvlist_duplicator(nvlist_t *src_nvl, void *user_data, fp_nvlist_handler nvlist_hdlr, nvlist_t **dest_nvl)
{
    int err = 0;
    nvlist_t *new_nvl = NULL;
    nvpair_t *nvp = NULL;

    err = nvlist_alloc(&new_nvl, NV_UNIQUE_NAME, 0);
    if (err)
    {
        ZLOG(ZLOG_MSG, "Couldn't allocate new name-value list\n");
        return err;
    }

    for (nvp = nvlist_next_nvpair(src_nvl, NULL);
         nvp != NULL;
         nvp = nvlist_next_nvpair(src_nvl, nvp))
    {
        err = nvlist_hdlr(src_nvl, nvp, user_data, new_nvl);
        if (err)
        {
            nvlist_free(new_nvl);
            return err;
        }
    }

    *dest_nvl = new_nvl;

    return err;
}

/**
 * new_property
 *
 *   replace the original snapshot's guid by the given BEGIN's toguid
 *   and return the new properties.
 *
 * @param   pcurr_begin     [in]  original drr BEGIN
 * @param   pnew_begin      [in]  new split BEGIN
 * @param   prop            [in]  original properties
 * @param   prop_len        [in]  original properties' length
 * @param   needswap        [in]  byte order needs to swap
 * @param   new_prop        [out] new properties' buffer
 * @param   new_prop_len    [out] new properties' length
 * @return  0 for success and other values for failure
 */
int
new_property(
    dmu_replay_record_t *pcurr_begin,
    dmu_replay_record_t *pnew_begin,
    void *prop,
    uint32_t prop_len,
    boolean needswap,
    void **new_prop,
    uint32_t *new_prop_len)
{
    int err = 0;
    nvlist_t *nvl = NULL;
    nvlist_t *new_nvl = NULL;
    prop_info_t prop_info = {0};
    size_t len = 0;

    prop_info.ori_toguid = pcurr_begin->drr_u.drr_begin.drr_toguid;
    prop_info.new_toguid = pnew_begin->drr_u.drr_begin.drr_toguid;
    prop_info.ori_toname = strchr(pcurr_begin->drr_u.drr_begin.drr_toname, '@');
    prop_info.new_toname = strchr(pnew_begin->drr_u.drr_begin.drr_toname, '@');
    prop_info.ori_toname++;
    prop_info.new_toname++;

    err = nvlist_unpack(prop, prop_len, &nvl, 0);
    if (err)
    {
        ZLOG(ZLOG_MSG, "Invalid stream (malformed nvlist)\n");
        return EINVAL;
    }

    err = nvlist_duplicator(nvl, &prop_info, begin_prop_hdlr, &new_nvl);
    if (err)
    {
        nvlist_free(new_nvl);
        return err;
    }

    err = nvlist_pack(new_nvl, (char **)new_prop, &len, NV_ENCODE_XDR, 0);
    if (err)
    {
        ZLOG(ZLOG_MSG, "Failed to add new properties\n");
        nvlist_free(new_nvl);
        return err;
    }

    *new_prop_len = len;

    return 0;
}

