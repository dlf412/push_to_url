#!/bin/bash


set -e
if [ $# -lt 1 ] ; then
    echo "$0 pkgname"
    exit 1
fi

pkg_root=$1
pkg_name="$pkg_root.tar.gz"

if [ -d $pkg_root ] ; then
  echo -n "$pkg_root exists, override? [y/n]"
  read flag
  if [ "$flag" == "n" ] ; then
    echo "abort"
    exit 1
  else
    rm -rf $pkg_root
  fi
fi

svn export . $1
rm $1/mkpkg.sh -f
tar czvf $pkg_name $pkg_root

