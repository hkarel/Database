/*****************************************************************************
  The MIT License

  Copyright © 2020 Pavel Karelin (hkarel), <hkarel@yandex.ru>

  Permission is hereby granted, free of charge, to any person obtaining
  a copy of this software and associated documentation files (the
  "Software"), to deal in the Software without restriction, including
  without limitation the rights to use, copy, modify, merge, publish,
  distribute, sublicense, and/or sell copies of the Software, and to
  permit persons to whom the Software is furnished to do so, subject to
  the following conditions:

  The above copyright notice and this permission notice shall be included
  in all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
  CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
  TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
  SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
*****************************************************************************/

#pragma once

//#include "firebird_driver.h"
#include "shared/defmac.h"
#include "shared/container_ptr.h"
#include "shared/logger/logger.h"
#include "shared/simple_timer.h"
#include "shared/thread/thread_utils.h"
#include "shared/qt/logger_operators.h"

#include <QMutex>
#include <string>
#include <functional>

#define log_error_m   alog::logger().error   (alog_line_location, "DbConnect")
#define log_warn_m    alog::logger().warn    (alog_line_location, "DbConnect")
#define log_info_m    alog::logger().info    (alog_line_location, "DbConnect")
#define log_verbose_m alog::logger().verbose (alog_line_location, "DbConnect")
#define log_debug_m   alog::logger().debug   (alog_line_location, "DbConnect")
#define log_debug2_m  alog::logger().debug2  (alog_line_location, "DbConnect")

namespace db {

/**
  Пул подключений к БД
*/
template<typename DatabaseT>
class ConnectPool
{
public:
    typedef std::function<bool (typename DatabaseT::Ptr)> InitFunc;

    ConnectPool() = default;

    // Таймаут задается в секундах
    bool init(InitFunc, int timeout = 10*60 /*10 мин*/);
    void close();

    void abortOperations();
    void abortOperation(pid_t threadId);

    typename DatabaseT::Ptr connect(int timeout = 0);

private:
    DISABLE_DEFAULT_COPY(ConnectPool)

    struct Data
    {
        typedef container_ptr<Data> Ptr;

        typename DatabaseT::Ptr driver;
        bool inUse = {false};
        pid_t threadId = {0};
        int timeout = {60}; /*60 сек*/
        simple_timer timer;
    };

    QList<typename Data::Ptr> _connectList;
    InitFunc _initFunc;
    QMutex _poolLock;

    // Таймаут по умолчанию (задается в секундах)
    int _defaultTimeout;

    template<typename T, int> friend T& ::safe_singleton();
};

template<typename DatabaseT>
bool ConnectPool<DatabaseT>::init(InitFunc initFunc, int timeout)
{
    _initFunc = initFunc;
    _defaultTimeout = timeout;
    //return true;

    typename DatabaseT::Ptr drv = connect();
    return drv->isOpen();
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
typename DatabaseT::Ptr ConnectPool<DatabaseT>::connect(int timeout)
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
                // Это условие  приведет к тому,  что в новом потоке будет
                // создаваться подключение, которое после окончания работы
                // потока будет "висеть" еще timeout сек,  и его уже никто
                // повторно не использует.
                // && d->threadId == threadId)
            {
                d->inUse = true;
                d->threadId = threadId;
                d->timeout = (timeout > 0) ? timeout : _defaultTimeout;

                driver = d->driver;
                driver->setThreadId(threadId); // Сделано для Postgres
                break;
            }

    if (driver.empty())
    {
        typename Data::Ptr d {new Data};
        d->driver = DatabaseT::create();
        d->inUse = true;
        d->threadId = threadId;
        d->timeout = (timeout > 0) ? timeout : _defaultTimeout;
        _connectList.append(d);

        driver = d->driver;
        driver->setThreadId(threadId); // Сделано для Postgres
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
            && d->timer.elapsed() > (d->timeout * 1000))
            //&& d->timer.elapsed() > 20*60*1000 /*20 мин*/)
            //&& d->timer.elapsed() > 15*1000)
        {
            d->driver->close();
            _connectList.removeAt(i--);
        }
    }

    if (driver && !driver->isOpen())
    {
        if (_initFunc == nullptr || !_initFunc(driver))
            driver->abortOperation();
    }
    return driver;
}

} // namespace db

#undef log_error_m
#undef log_warn_m
#undef log_info_m
#undef log_verbose_m
#undef log_debug_m
#undef log_debug2_m
