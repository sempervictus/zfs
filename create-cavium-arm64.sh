#!/bin/sh
#
export HOST="aarch64-unknown-linux-gnu"
export BUILD="aarch64-unknown-linux-gnu"
export LIB_DIR_NAME="aarch64-linux-gnu"
cd ../spl
patch -N -r - < deb.am.arm64.patch config/deb.am
cd -
patch -N -r - < deb.am.arm64.patch config/deb.am
patch -N -r - < time.h.arm64.patch lib/libspl/include/sys/time.h
patch -N -r - < zio.c.arm64.patch module/zfs/zio.c

sh ./create-build.sh $1
