#include "pqserver.hh"

namespace pq {

} // namespace

int main(int argc, char** argv) {
    pq::Server server;

    const char *keys[] = {
	"a", "ab", "ac"
    }, *values[] = {
	"x", "mn", "jioasfhsanjnsda"
    };
    server.replace_range(keys, keys + 3, values);
}
