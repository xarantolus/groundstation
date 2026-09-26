// Encode CCSDS TM frames exactly as the STS1 flight software does, using its own
// ChannelCoding sources (Scrambler, ConvolutionalCoding) and libfec's encode_rs_ccsds
// (which is what Sts1CobcSw/ChannelCoding/ReedSolomon.cpp calls).
//
// stdin: one 223-byte TM frame per line (hex). stdout: the 520 on-air bytes per line (hex),
// i.e. what is handed to the Si4463 after its 0x55 preamble.
#include <Sts1CobcSw/ChannelCoding/External/ConvolutionalCoding.hpp>
#include <Sts1CobcSw/ChannelCoding/Scrambler.hpp>

extern "C" {
#include <libfec/fec.h>
}

#include <array>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

using sts1cobcsw::Byte;

int main()
{
    constexpr auto asm_ = std::array<unsigned char, 4>{0x1A, 0xCF, 0xFC, 0x1D};
    std::string line;
    while(std::getline(std::cin, line))
    {
        if(line.size() != 2 * 223)
        {
            std::fprintf(stderr, "skipping line of length %zu\n", line.size());
            continue;
        }
        auto cadu = std::array<unsigned char, 4 + 255>{};
        std::copy(asm_.begin(), asm_.end(), cadu.begin());
        for(auto i = 0U; i < 223; ++i)
        {
            cadu[4 + i] = static_cast<unsigned char>(std::stoul(line.substr(2 * i, 2), nullptr, 16));
        }
        // tm::Encode(): rs::Encode (encode_rs_ccsds) then Scramble over the 255-byte block
        encode_rs_ccsds(&cadu[4], &cadu[4 + 223], 0);
        auto block = std::span(reinterpret_cast<Byte *>(&cadu[4]), 255);
        sts1cobcsw::tm::Scramble(block);
        // RfCommunicationThread: convolutionally encode ASM + block with flush
        auto codec = sts1cobcsw::cc::ViterbiCodec();
        auto encoded =
            codec.Encode(std::span(reinterpret_cast<Byte const *>(cadu.data()), cadu.size()), true);
        for(auto b : encoded)
        {
            std::printf("%02x", static_cast<unsigned>(b));
        }
        std::printf("\n");
    }
}
