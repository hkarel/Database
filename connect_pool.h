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
    static const int DEFAULT_TIMEOUT = 10*60; /*10 мин*/
    typedef std::function<bool (typename DatabaseT::Ptr)> InitFunc;

    ConnectPool() = default;

    // Параметр defaultTimeout задает время по умолчанию  (в секундах)
    // по истечении которого соединение с БД будет закрыто при условии
    // бездействия этого соединения
    bool init(InitFunc, int defaultTimeout = DEFAULT_TIMEOUT);
    void close();

    void abortOperations();
    void abortOperation(pid_t threadId);

    // Параметр timeout задает время (в секундах)  по истечении  которого
    // соединение с  БД  будет  закрыто  при  условии  бездействия  этого
    // соединения. Если timeout меньше или равно 0, то в качестве таймаута
    // будет использоваться defaultTimeout
    typename DatabaseT::Ptr connect(int timeout = 0);

    // Определяет режим создания нового подключения к базе данных. По умолчанию
    // пул коннектов создает в одном потоке исполнения только одно  подключение
    // к базе данных. Для некоторых драйверов БД (Postgres) это является  проб-
    // лемой, так как они не могут создавать несколько  экземпляров  транзакций
    // в рамках одного подключения. Рассмотрим  пример  использования  Postgres
    // драйвера:
    //   func() {
    //     PgDriver con1 = pgpool.connect();
    //     QSqlQuery q1 {con1->createResult()}; - внутри создается транзакция
    //     ...
    //     PgDriver con2 = pgpool.connect();
    //     QSqlQuery q2 {con2->createResult()}; - внутри создается транзакция
    //     ...
    //   }
    // Здесь sql-запрос  не будет  корректно  выполняться  для  компонента  q2,
    // так как con1 и con2  ссылаются  на  одно  подключение к БД, и экземпляр
    // транзакции уже неявно создан  для  компонента q1.  Установка  параметра
    // singleConnection в FALSE позволит при каждом  вызове  метода connect()
    // создавать новое подключении к БД, таким образом пример будет корректно
    // работать.
    // По умолчанию параметр равен TRUE (одни поток - одно подключение к БД)
    bool singleConnection() const;
    void setSingleConnection(bool);

private:
    DISABLE_DEFAULT_COPY(ConnectPool)

    struct Data
    {
        typedef container_ptr<Data> Ptr;

        typename DatabaseT::Ptr driver;
        bool inUse = {false};
        pid_t threadId = {0};
        simple_timer timer;
        int timeout = {DEFAULT_TIMEOUT};
    };

    QMutex _poolLock;
    InitFunc _initFunc;
    bool _singleConnection = {true};
    int _defaultTimeout = {DEFAULT_TIMEOUT};

    QList<typename Data::Ptr> _connectList;

    template<typename T, int> friend T& ::safe_singleton();
};

template<typename DatabaseT>
bool ConnectPool<DatabaseT>::init(InitFunc initFunc, int defaultTimeout)
{
    { //Block for QMutexLocker
        QMutexLocker locker {&_poolLock}; (void) locker;
        _initFunc = initFunc;
        _defaultTimeout = defaultTimeout;
    }
    typename DatabaseT::Ptr drv = connect();
    return drv->isOpen();
}

template<typename DatabaseT>
void ConnectPool<DatabaseT>::close()
{
    QMutexLocker locker {&_poolLock}; (void) locker;

    log_verbose_m << "Close database pool connection"
                  << ". Pool size: " << _connectList.count();

    for (typename Data::Ptr& d : _connectList)
        d->driver->close();

    _connectList.clear();
}

template<typename DatabaseT>
void ConnectPool<DatabaseT>::abortOperations()
{
    QMutexLocker locker {&_poolLock}; (void) locker;

    log_verbose_m << "Abort database operations";

    for (typename Data::Ptr& d : _connectList)
        d->driver->abortOperation();
}

template<typename DatabaseT>
void ConnectPool<DatabaseT>::abortOperation(pid_t threadId)
{
    QMutexLocker locker {&_poolLock}; (void) locker;

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
    QMutexLocker locker {&_poolLock}; (void) locker;

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

    if (_singleConnection)
    {
        // В каждом потоке используем только одно подключение к БД
        for (const typename Data::Ptr& d : _connectList)
            if (d->threadId == threadId
                && d->driver->clife_count() > 1)
            {
                driver = d->driver;
                break;
            }
    }

    if (driver.empty())
        for (typename Data::Ptr& d : _connectList)
            if (d->driver->clife_count() == 1)
                // При использовании условия d->threadId == threadId подключе-
                // ние, созданное в новом потоке, "зависнет" на timeout секунд
                // после окончания работы потока. Затем оно будет закрыто.
                // Из других потоков это подключение не будет доступно, то есть
                // воспользоваться им повторно не получится.
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

template<typename DatabaseT>
bool ConnectPool<DatabaseT>::singleConnection() const
{
    QMutexLocker locker {&_poolLock}; (void) locker;
    return _singleConnection;
}

template<typename DatabaseT>
void ConnectPool<DatabaseT>::setSingleConnection(bool val)
{
    QMutexLocker locker {&_poolLock}; (void) locker;
    _singleConnection = val;
}

} // namespace db

#undef log_error_m
#undef log_warn_m
#undef log_info_m
#undef log_verbose_m
#undef log_debug_m
#undef log_debug2_m
