#include "connect.h"
#include "shared/qt/config/config.h"
#include <QtCore>
#include <QHostAddress>
#include <QMutexLocker>

#define log_error_m   alog::logger().error  (__FILE__, __func__, __LINE__, "DbConnect")
#define log_warn_m    alog::logger().warn   (__FILE__, __func__, __LINE__, "DbConnect")
#define log_info_m    alog::logger().info   (__FILE__, __func__, __LINE__, "DbConnect")
#define log_verbose_m alog::logger().verbose(__FILE__, __func__, __LINE__, "DbConnect")
#define log_debug_m   alog::logger().debug  (__FILE__, __func__, __LINE__, "DbConnect")
#define log_debug2_m  alog::logger().debug2 (__FILE__, __func__, __LINE__, "DbConnect")

bool DatabasePool::init()
{
    db::postgres::Driver::Ptr drv = connect();
    return drv->isOpen();
}

void DatabasePool::close()
{
    QMutexLocker locker(&_poolLock); (void) locker;

    log_verbose_m << "Close database pool connection"
                  << ". Pool size: " << _connectList.count();

    for (Data::Ptr& d : _connectList)
        d->driver->close();

    _connectList.clear();
}

void DatabasePool::abortOperations()
{
    QMutexLocker locker(&_poolLock); (void) locker;

    log_verbose_m << "Abort database operations";

    for (Data::Ptr& d : _connectList)
        d->driver->abortOperation();
}

void DatabasePool::abortOperation(pid_t threadId)
{
    QMutexLocker locker(&_poolLock); (void) locker;

    for (Data::Ptr& d : _connectList)
        if (d->threadId == threadId)
        {
            log_verbose_m << "Abort sql-operation (for thread: " << threadId << ")";
            d->driver->abortOperation();
        }
}

db::postgres::Driver::Ptr DatabasePool::connect()
{
    QMutexLocker locker(&_poolLock); (void) locker;

    // Проверяем коннекты для которых операция была прервана, их удаляем
    for (int i = 0; i < _connectList.count(); ++i)
    {
        const Data::Ptr& d = _connectList[i];
        if (d->driver->operationIsAborted())
        {
            if (d->driver->clife_count() == 1)
                d->driver->close();
            _connectList.removeAt(i--);
        }
    }

    pid_t threadId = trd::gettid();
    db::postgres::Driver::Ptr driver;

    // В каждом потоке используем только одно подключение к БД
    for (const Data::Ptr& d : _connectList)
        if (d->threadId == threadId
            && d->driver->clife_count() > 1)
        {
            driver = d->driver;
            break;
        }

    if (driver.empty())
        for (Data::Ptr& d : _connectList)
            if (d->driver->clife_count() == 1)
            {
                driver = d->driver;
                d->inUse = true;
                d->threadId = threadId;
                break;
            }

    if (driver.empty())
    {
        Data::Ptr d {new Data};
        d->driver = db::postgres::Driver::create();
        d->inUse = true;
        d->threadId = threadId;
        _connectList.append(d);
        driver = d->driver;
    }

    // Проверяем неиспользуемые коннекты
    for (Data::Ptr& d : _connectList)
        if (d->driver->clife_count() == 1
            && d->inUse == true)
        {
            d->inUse = false;
            d->timer.reset();
        }

    // Удаляем неиспользуемые коннекты
    for (int i = 0; i < _connectList.count(); ++i)
    {
        const Data::Ptr& d = _connectList[i];
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
        if (!databaseInit(driver))
            driver->abortOperation();
    }
    return driver;
}

DatabasePool& dbpool()
{
    return ::safe_singleton<DatabasePool>();
}

bool databaseInit(db::postgres::Driver::Ptr& drv)
{
    QString hostAddress = "127.0.0.1";
    int port = 3050;
    QString user = "SYSDBA";
    QString password = "masterkey";
    QString file;

    YamlConfig::Func loadFunc = [&](YamlConfig* conf, YAML::Node& node, bool /*logWarn*/)
    {
        conf->getValue(node, "address", hostAddress);
        conf->getValue(node, "port", port);
        conf->getValue(node, "user", user);
        conf->getValue(node, "password", password);
#ifndef MINGW
        conf->getValue(node, "file", file);
#else
        conf->getValue(node, "file_win", file);
        config::dirExpansion(file);
#endif
        return true;
    };
    config::base().getValue("database", loadFunc);
    return drv->open(file, user, password, hostAddress, port);
}

#undef log_error_m
#undef log_warn_m
#undef log_info_m
#undef log_verbose_m
#undef log_debug_m
#undef log_debug2_m
