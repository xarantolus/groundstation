#!/usr/bin/env bash
# Builds fw_encoder from the STS1 flight software sources (fetched into ./deps).
set -euo pipefail
cd "$(dirname "$0")"
mkdir -p deps && cd deps
[ -d STS1_COBC_SW ] || git clone -q --depth 1 https://github.com/SpaceTeam/STS1_COBC_SW.git
[ -d etl ] || git clone -q --depth 1 https://github.com/ETLCPP/etl.git
[ -d libfec ] || git clone -q --depth 1 https://github.com/quiet/libfec.git
cd libfec
if [ ! -f ccsds_tab.c ]; then
    gcc -O2 -o gen_ccsds gen_ccsds.c init_rs_char.c && ./gen_ccsds > ccsds_tab.c
    gcc -O2 -o gen_ccsds_tal gen_ccsds_tal.c && ./gen_ccsds_tal > ccsds_tal.c
fi
cd ../..
gcc -O2 -c deps/libfec/encode_rs_ccsds.c deps/libfec/encode_rs_8.c deps/libfec/ccsds_tab.c deps/libfec/ccsds_tal.c
g++ -std=c++20 -O2 -DNDEBUG \
    -Ideps/STS1_COBC_SW -Ideps/etl/include -Ideps \
    main.cpp \
    deps/STS1_COBC_SW/Sts1CobcSw/ChannelCoding/External/ConvolutionalCoding.cpp \
    deps/STS1_COBC_SW/Sts1CobcSw/ChannelCoding/Scrambler.cpp \
    encode_rs_ccsds.o encode_rs_8.o ccsds_tab.o ccsds_tal.o \
    -o fw_encoder
rm -f *.o
echo "built $(pwd)/fw_encoder"
