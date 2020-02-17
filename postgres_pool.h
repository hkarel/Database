#pragma once

#include "postgres_driver.h"
#include "connect_pool.h"

namespace db {
namespace postgres {

typedef ConnectPool<Driver>  Pool;

Pool& pool();

} // namespace postgres
} // namespace db

inline db::postgres::Pool& pgpool() {return db::postgres::pool();}
