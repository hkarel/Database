#include "firebird_pool.h"
#include "shared/safe_singleton.h"

namespace db {
namespace firebird {

Pool& pool()
{
    return ::safe_singleton<Pool>();
}

} // namespace firebird
} // namespace db

