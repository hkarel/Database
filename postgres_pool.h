#pragma once

#include "connect_pool.h"
#include "postgres_driver.h"

namespace db {
namespace postgres {

typedef ConnectPool<Driver>  Pool;

Pool& pool();

} // namespace postgres
} // namespace db
