#include "kvstore/cli.hpp"

#include <iostream>
#include <utility>
#include <unistd.h>

int main(int argc, char** argv) {
    kvstore::CliConfig config;
    const kvstore::Status parse_status = kvstore::ParseCliArgs(argc, argv, &config);
    if (!parse_status.ok()) {
        std::cerr << "Error: " << parse_status.message << '\n';
        kvstore::PrintCliUsage(std::cerr);
        return 1;
    }
    if (config.show_help) {
        kvstore::PrintCliUsage(std::cout);
        return 0;
    }

    kvstore::CliSession session(std::move(config));
    const kvstore::Status open_status = session.Open();
    if (!open_status.ok()) {
        std::cerr << "Error: " << open_status.message << '\n';
        return 1;
    }

    const bool interactive = ::isatty(STDIN_FILENO) != 0;
    return session.Run(std::cin, std::cout, std::cerr, interactive);
}
