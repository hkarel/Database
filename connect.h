#pragma once

#include "postgres_driver.h"
#include "shared/container_ptr.h"
#include "shared/simple_timer.h"
#include "shared/safe_singleton.h"
#include "shared/thread/thread_info.h"

#include <QMutex>
#include <string>

/**
  Пул подключений к БД
*/
class DatabasePool
{
public:
    bool init();
    void close();

    void abortOperations();
    void abortOperation(pid_t threadId);

    db::postgres::Driver::Ptr connect();

private:
    DatabasePool() = default;
    DISABLE_DEFAULT_COPY(DatabasePool)

    struct Data
    {
        typedef container_ptr<Data> Ptr;

        db::postgres::Driver::Ptr driver;
        bool inUse = {false};
        pid_t threadId = {0};
        simple_timer timer;
    };

    QList<Data::Ptr> _connectList;
    QMutex _poolLock;

    template<typename T, int> friend T& ::safe_singleton();
};

DatabasePool& dbpool();
bool databaseInit(db::postgres::Driver::Ptr&);
