#pragma once

//#include "firebird_driver.h"
#include "shared/defmac.h"
#include "shared/container_ptr.h"
#include "shared/logger/logger.h"
#include "shared/simple_timer.h"
#include "shared/thread/thread_info.h"
#include "shared/qt/logger/logger_operators.h"

#include <QMutex>
#include <string>
#include <functional>

#define log_error_m   alog::logger().error  (__FILE__, __func__, __LINE__, "DbConnect")
#define log_warn_m    alog::logger().warn   (__FILE__, __func__, __LINE__, "DbConnect")
#define log_info_m    alog::logger().info   (__FILE__, __func__, __LINE__, "DbConnect")
#define log_verbose_m alog::logger().verbose(__FILE__, __func__, __LINE__, "DbConnect")
#define log_debug_m   alog::logger().debug  (__FILE__, __func__, __LINE__, "DbConnect")
#define log_debug2_m  alog::logger().debug2 (__FILE__, __func__, __LINE__, "DbConnect")

namespace db {

/**
  Пул подключений к БД
*/
template<typename DatabaseT>
class ConnectPool
{
public:
    typedef std::function<bool (DatabaseT&)> InitFunc;

    ConnectPool() = default;

    bool init(InitFunc);
    void close();

    void abortOperations();
    void abortOperation(pid_t threadId);

    typename DatabaseT::Ptr connect();

private:
    DISABLE_DEFAULT_COPY(ConnectPool)

    struct Data
    {
        typedef container_ptr<Data> Ptr;

        typename DatabaseT::Ptr driver;
        bool inUse = {false};
        pid_t threadId = {0};
        simple_timer timer;
    };

    QList<typename Data::Ptr> _connectList;
    InitFunc _initFunc;
    QMutex _poolLock;

    template<typename T, int> friend T& ::safe_singleton();
};

template<typename DatabaseT>
bool ConnectPool<DatabaseT>::init(InitFunc initFunc)
{
    _initFunc = initFunc;
    return true;

    //typename DatabaseT::Ptr drv = connect();
    //return drv->isOpen();
}

template<typename DatabaseT>
void ConnectPool<DatabaseT>::close()
{
    QMutexLocker locker(&_poolLock); (void) locker;

    log_verbose_m << "Close database pool connection"
                  << ". Pool size: " << _connectList.count();

    for (typename Data::Ptr& d : _connectList)
        d->driver->close();

    _connectList.clear();
}

template<typename DatabaseT>
void ConnectPool<DatabaseT>::abortOperations()
{
    QMutexLocker locker(&_poolLock); (void) locker;

    log_verbose_m << "Abort database operations";

    for (typename Data::Ptr& d : _connectList)
        d->driver->abortOperation();
}

template<typename DatabaseT>
void ConnectPool<DatabaseT>::abortOperation(pid_t threadId)
{
    QMutexLocker locker(&_poolLock); (void) locker;

    for (typename Data::Ptr& d : _connectList)
        if (d->threadId == threadId)
        {
            log_verbose_m << "Abort sql-operation (for thread: " << threadId << ")";
            d->driver->abortOperation();
        }
}

template<typename DatabaseT>
typename DatabaseT::Ptr ConnectPool<DatabaseT>::connect()
{
    QMutexLocker locker(&_poolLock); (void) locker;

    // Проверяем коннекты для которых операция была прервана, их удаляем
    for (int i = 0; i < _connectList.count(); ++i)
    {
        const typename Data::Ptr& d = _connectList[i];
        if (d->driver->operationIsAborted())
        {
            if (d->driver->clife_count() == 1)
                d->driver->close();
            _connectList.removeAt(i--);
        }
    }

    pid_t threadId = trd::gettid();
    typename DatabaseT::Ptr driver;

    // В каждом потоке используем только одно подключение к БД
    for (const typename Data::Ptr& d : _connectList)
        if (d->threadId == threadId
            && d->driver->clife_count() > 1)
        {
            driver = d->driver;
            break;
        }

    if (driver.empty())
        for (typename Data::Ptr& d : _connectList)
            if (d->driver->clife_count() == 1)
            {
                driver = d->driver;
                d->inUse = true;
                d->threadId = threadId;
                break;
            }

    if (driver.empty())
    {
        typename Data::Ptr d {new Data};
        d->driver = DatabaseT::create();
        d->inUse = true;
        d->threadId = threadId;
        _connectList.append(d);
        driver = d->driver;
    }

    // Проверяем неиспользуемые коннекты
    for (typename Data::Ptr& d : _connectList)
        if (d->driver->clife_count() == 1
            && d->inUse == true)
        {
            d->inUse = false;
            d->timer.reset();
        }

    // Удаляем неиспользуемые коннекты
    for (int i = 0; i < _connectList.count(); ++i)
    {
        const typename Data::Ptr& d = _connectList[i];
        if (d->driver->clife_count() == 1
            && d->inUse == false
            && d->timer.elapsed() > 20*60*1000 /*20 мин*/)
            //&& d->timer.elapsed() > 15*1000)
        {
            d->driver->close();
            _connectList.removeAt(i--);
        }
    }

    if (driver && !driver->isOpen())
    {
        if (_initFunc.empty() || !_initFunc(driver))
            driver->abortOperation();
    }
    return driver;
}


//ConnectPool& dbpool();

} // namespace db

#undef log_error_m
#undef log_warn_m
#undef log_info_m
#undef log_verbose_m
#undef log_debug_m
#undef log_debug2_m
