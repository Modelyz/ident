#!/bin/bash

OPT_DEVEL='--ghc-options="-Wall"'
OPT_OPTIMIZE=' -O2 --ghc-options="-Wall" --enable-split-objs --enable-executable-stripping --enable-library-stripping --enable-executable-static'
INSTALLDIR=build
OPT_INSTALL="--installdir=$INSTALLDIR --overwrite-policy=always --install-method=copy"

set -e

pushd $( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
mkdir -p $INSTALLDIR

if [ "$1" == "-o" ]; then
    # TODO stripping does not work
    cabal build $OPT_OPTIMIZE
    cabal install $OPT_OPTIMIZE $OPT_INSTALL
    #cabal v1-install -O2 --enable-split-objs --enable-executable-static --enable-executable-stripping --bindir=$INSTALLDIR
else
    cabal build $OPT_DEVEL
    cabal install $OPT_DEVEL $OPT_INSTALL
fi
popd
