#pragma once

#include "firebird_driver.h"
#include "connect_pool.h"

namespace db {
namespace firebird {

typedef ConnectPool<Driver> Pool;

Pool& pool();

} // namespace firebird
} // namespace db

inline db::firebird::Pool& fbpool() {return db::firebird::pool();}
