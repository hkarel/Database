#include "postgres_pool.h"
#include "shared/safe_singleton.h"

namespace db {
namespace postgres {

Pool& pool()
{
    return ::safe_singleton<Pool>();
}

} // namespace postgres
} // namespace db

