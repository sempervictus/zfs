#!/bin/sh
#
set -x
set -e
#
KERNEL_VERSION=`uname -r`
if [ "${HOST}" = "" ]
then
    HOST="x86_64-pc-linux-gnu"
fi

if [ "${BUILD}" = "" ]
then
    BUILD="x86_64-pc-linux-gnu"
fi

if [ "${LIB_DIR_NAME}" = "" ]
then
    LIB_DIR_NAME="x86_64-linux-gnu"
fi

#
if [ "$1" = "" ]
then
    echo "Usage: $0 [build_no]"
    exit 1
fi
BUILD_NUMBER="$1"
ZFS_VERSION="`cat META | grep ^Version: | tr '+' ' ' | awk '{print $2}'`"
############################################
# SPL
#
cd ../spl/
# cleanup
if [ -f "Makefile" ]
then
    make distclean
fi
rm -fv *.deb *.rpm spl*.tar.gz
# config
(grep -v "^Version:" META; echo "Version: ${ZFS_VERSION}+PDS${BUILD_NUMBER}") > META.new
mv -v META.new META
cat META
./autogen.sh
./configure --host=${HOST} --build=${BUILD} \
  --program-prefix= --disable-dependency-tracking --prefix=/usr --exec-prefix=/usr \
  --bindir=/usr/bin --sbindir=/usr/sbin --sysconfdir=/etc --datadir=/usr/share \
  --includedir=/usr/include --libdir=/usr/lib64 --libexecdir=/usr/lib/${LIB_DIR_NAME} \
  --localstatedir=/var --sharedstatedir=/usr/com --mandir=/usr/share/man \
  --infodir=/usr/share/info --with-config=all \
  --with-linux=/lib/modules/${KERNEL_VERSION}/build --with-linux-obj=/lib/modules/${KERNEL_VERSION}/build \
  --disable-debug --enable-debug-kmem --disable-debug-kmem-tracking --disable-atomic-spinlocks
# create build
make deb
make  # create build env for zfs
cd -

############################################
# ZFS
#
# cleanup
if [ -f "Makefile" ]
then
    make distclean
fi
rm -fv *.deb *.rpm zfs*.tar.gz
# config
(grep -v "^Version:" META; echo "Version: ${ZFS_VERSION}+PDS${BUILD_NUMBER}") > META.new
mv -v META.new META
cat META
./autogen.sh
SPL_PATH="`realpath ../spl`"
./configure --host=${HOST} --build=${BUILD} \
  --program-prefix= --disable-dependency-tracking --prefix=/usr --exec-prefix=/usr \
  --bindir=/usr/bin --sbindir=/usr/sbin --sysconfdir=/etc --datadir=/usr/share \
  --includedir=/usr/include --libdir=/usr/lib64 --libexecdir=/usr/lib/${LIB_DIR_NAME} \
  --localstatedir=/var --sharedstatedir=/usr/com --mandir=/usr/share/man \
  --infodir=/usr/share/info --with-config=all \
  --with-linux=/lib/modules/${KERNEL_VERSION}/build --with-linux-obj=/lib/modules/${KERNEL_VERSION}/build \
  --with-spl=${SPL_PATH} --with-spl-obj=${SPL_PATH} \
  --disable-debug --disable-debug-dmu-tx
make deb

# Create zstream_split
echo "=================================================================="
make distclean
# avoid zstream_split include into deb package
sed -i -e 's/#sbin_PROGRAMS/sbin_PROGRAMS/g' cmd/zstream_split/Makefile.am
./autogen.sh
./configure --libdir=/lib64
make -C lib
make -C cmd/zstream_split
cp -fv ./cmd/zstream_split/.libs/zstream_split .

# Revert change of above - avoid zstream_split include into deb package
sed -i -e 's/sbin_PROGRAMS/#sbin_PROGRAMS/g' cmd/zstream_split/Makefile.am

# Prepare deb releases
rm -rf debs
mkdir -p debs
cp -fv ../spl/spl_*+PDS${BUILD_NUMBER}*.deb ../spl/kmod-spl-${KERNEL_VERSION}_${ZFS_VERSION}+PDS${BUILD_NUMBER}*.deb debs/
for pkg in libnvpair1 libzpool2 libuutil1 libzfs2 zfs kmod-zfs-${KERNEL_VERSION}
do
   cp -fv ${pkg}_${ZFS_VERSION}+PDS${BUILD_NUMBER}*.deb debs/
done

exit 0
