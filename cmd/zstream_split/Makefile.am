include $(top_srcdir)/config/Rules.am

DEFAULT_INCLUDES += \
	-I$(top_srcdir)/include \
	-I$(top_srcdir)/lib/libspl/include

#sbin_PROGRAMS = zstream_split

zstream_split_SOURCES = \
	$(top_srcdir)/cmd/zstream_split/zcut_util.h \
	$(top_srcdir)/cmd/zstream_split/zcut_util.c \
	$(top_srcdir)/cmd/zstream_split/zcut_prop.h \
	$(top_srcdir)/cmd/zstream_split/zcut_prop.c \
	$(top_srcdir)/cmd/zstream_split/zcut_split.h \
	$(top_srcdir)/cmd/zstream_split/zcut_split.c \
	$(top_srcdir)/cmd/zstream_split/zcut_drr.h \
	$(top_srcdir)/cmd/zstream_split/zcut_drr.c \
	$(top_srcdir)/cmd/zstream_split/zcut_dbg.h \
	$(top_srcdir)/cmd/zstream_split/zcut_partname.h \
	$(top_srcdir)/cmd/zstream_split/zcut.h \
	$(top_srcdir)/cmd/zstream_split/zcut.c \
        $(top_srcdir)/cmd/zstream_split/zcut_dbg.h \
        $(top_srcdir)/cmd/zstream_split/zcut_partname.h

zstream_split_LDADD = \
	$(top_builddir)/lib/libnvpair/libnvpair.la \
	$(top_builddir)/lib/libuutil/libuutil.la \
	$(top_builddir)/lib/libzpool/libzpool.la \
	$(top_builddir)/lib/libzfs/libzfs.la

zstream_split_LDFLAGS = -pthread -lm $(ZLIB) -lrt -ldl $(LIBUUID) $(LIBBLKID)
