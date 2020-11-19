#pragma once
#include "shared/config/yaml_config.h"

namespace db {
namespace firebird {

/**
  Проверка асинхронного режима работы БД
*/
bool asyncCheck(const YamlConfig&);

} // namespace firebird
} // namespace db
