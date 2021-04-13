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

#include "qmetatypes.h"
#include "sqlcachedresult.h"
#include "shared/defmac.h"
#include "shared/clife_base.h"
#include "shared/clife_ptr.h"
#include "shared/container_ptr.h"
#include "shared/qt/quuidex.h"

#include <QSqlError>
#include <QSqlDriver>
#include <QSqlResult>

#include <libpq-fe.h>
#include <atomic>

namespace db {

template<typename> class ConnectPool;

namespace postgres {

class Result;
class Driver;
typedef clife_ptr<Driver> DriverPtr;

template<typename> struct PGresultDestroy
{
    static void destroy(PGresult* result) {PQclear(result);}
};
typedef container_ptr<PGresult, PGresultDestroy> PGresultPtr;

class Transaction final : public clife_base
{
public:
    typedef clife_ptr<Transaction> Ptr;

    enum class WritePolicy
    {
        ReadOnly  = 0,
        ReadWrite = 1
    };

    /** Transaction isolation levels (See libpqxx, file isolation.hxx) **
      These are as defined in the SQL standard.  But there are a few notes
      specific to PostgreSQL.

      First, postgres does not support "read uncommitted."  The lowest level you
      can get is "read committed," which is better.  PostgreSQL is built on the
      MVCC paradigm, which guarantees "read committed" isolation without any
      additional performance overhead, so there was no point in providing the
      lower level.

      Second, "repeatable read" also makes more isolation guarantees than the
      standard requires.  According to the standard, this level prevents "dirty
      reads" and "nonrepeatable reads," but not "phantom reads."  In postgres,
      it actually prevents all three.

      Third, "serializable" is only properly supported starting at postgres 9.1.
      If you request "serializable" isolation on an older backend, you will get
      the same isolation as in "repeatable read."  It's better than the "repeatable
      read" defined in the SQL standard, but not a complete implementation of the
      standard's "serializable" isolation level.

      In general, a lower isolation level will allow more surprising interactions
      between ongoing transactions, but improve performance.  A higher level
      gives you more protection from subtle concurrency bugs, but sometimes it
      may not be possible to complete your transaction without avoiding paradoxes
      in the data.  In that case a transaction may fail, and the application will
      have to re-do the whole thing based on the latest state of the database.
      (If you want to retry your code in that situation, have a look at the
      transactor framework.)

      Study the levels and design your application with the right level in mind.
    */
    enum class IsolationLevel
    {
        // PostgreSQL only has the better isolation levels.
        // read_uncommitted,
        ReadCommitted,
        RepeatableRead,
        Serializable
    };

    ~Transaction();

    bool begin(IsolationLevel = IsolationLevel::ReadCommitted,
               WritePolicy = WritePolicy::ReadWrite);
    bool commit();
    bool rollback();
    bool isActive() const;

private:
    Transaction(const DriverPtr&);
    DISABLE_DEFAULT_FUNC(Transaction)
    quint64 transactId() const {return _transactId;}

private:
    DriverPtr _drv;
    bool      _isActive = {false};
    quint64   _transactId = quint64(-1);

    friend class Result;
    friend class Driver;
    friend Transaction::Ptr createTransact(const DriverPtr&);
};

class Result final : public SqlCachedResult /*QSqlResult*/
{
public:
    enum class ForwardOnly {No = 0, Yes = 1};

    Result(const DriverPtr&, ForwardOnly);
    Result(const Transaction::Ptr&, ForwardOnly);
    ~Result();

    bool prepare(const QString& query) override;
    bool exec() override;

    /**
      Функция получает на входе буфер в виде строки, в которой содержатся
      данные в формате csv, и выполняет экспорт данных в таблицу.
      table - имя таблицы
      colums - список столбцов csv и таблицы
      buffer - буфер данных
    */
    bool copyInsert(const QString& table, const QList<QString>& columns, const QString& buffer);

protected:
    bool gotoNext(SqlCachedResult::ValueCache& row, int rowIdx) override;
    bool reset(const QString& query) override;
    int  size() override;
    int  numRowsAffected() override;
    QSqlRecord record() const override;

private:
    // Возвращает TRUE если sql-выражение  является  SELECT-запросом  или если
    // в sql-выражении вызывается хранимая процедура возвращающая набор данных.
    bool isSelectSql() const;

    void cleanup();
    bool checkError(const char* msg, QSqlError::ErrorType,
                    const PGresult*, const char* func, int line);

    bool beginInternalTransact();
    bool commitInternalTransact();
    bool rollbackInternalTransact();
    quint64 transactId() const;

private:
    DISABLE_DEFAULT_COPY(Result)

    DriverPtr        _drv;
    Transaction::Ptr _externalTransact;
    Transaction::Ptr _internalTransact;
    QByteArray       _stmtName;
    PGresultPtr      _stmt;
    QString          _preparedQuery; // Содержит подготовленный запрос
};

class Driver final : public QSqlDriver, public clife_base
{
public:
    typedef DriverPtr Ptr;
    ~Driver();

    static Ptr create();

    bool open(const QString& db,
              const QString& user,
              const QString& password,
              const QString& host,
              int   port,
              const QString& connOpts) override;

    bool open(const QString& db,
              const QString& user,
              const QString& password,
              const QString& host,
              int   port);

    void close() override;
    bool isOpen() const override;

    // Идентификатор потока в котором было установлено соединение с БД
    pid_t threadId() const {return _threadId;}

    Transaction::Ptr createTransact() const;

    QSqlResult* createResult() const override;
    QSqlResult* createResult(const Transaction::Ptr&) const;

    bool hasFeature(DriverFeature f) const override;

//    QStringList tables(QSql::TableType) const override;
//    QSqlRecord  record(const QString& tableName) const override;
//    QSqlIndex   primaryIndex(const QString& tableName) const override;

    QString formatValue(const QSqlField& field, bool trimStrings) const override;
    QString escapeIdentifier(const QString& identifier, IdentifierType type) const override;

    // Функция прерывает выполнение текущей sql-операции. Этой функцией нужно
    // пользоваться  когда  необходимо  экстренно  закрыть  подключение  к БД
    // не дожидаясь окончания выполнения sql-запроса. После того как операция
    // будет  прервана - данным  подключением  уже нельзя  будет пользоваться,
    // его можно будет только закрыть.
    void abortOperation();

    // Возвращает TRUE если sql-операция была прервана
    bool operationIsAborted() const;

//#if QT_VERSION >= 0x050000
//protected:
//    bool subscribeToNotification(const QString& name) override;
//    bool unsubscribeFromNotification(const QString& name) override;
//    QStringList subscribedToNotifications() const override;
//#endif
//protected slots:
//    bool subscribeToNotificationImplementation(const QString &name);
//    bool unsubscribeFromNotificationImplementation(const QString &name);
//    QStringList subscribedToNotificationsImplementation() const;

private:
    void setOpen(bool) override;

    bool beginTransaction() override;
    bool commitTransaction() override;
    bool rollbackTransaction() override;

    void setThreadId(pid_t threadId) {_threadId = threadId;}

    // Вспомогательные функции, используются для предотвращения одновременного
    // использования более чем одной транзакции с текущим подключением к БД
    void captureTransactAddr(Transaction*);
    void releaseTransactAddr(Transaction*);
    bool transactAddrIsEqual(Transaction*);

private:
    Q_OBJECT
    Driver();
    DISABLE_DEFAULT_COPY(Driver)

    PGconn* _connect = {nullptr};
    pid_t   _threadId = {0};
    quint64 _transactNumber = {0};
    std::atomic_bool _operationIsAborted = {false};

    friend class Result;
    friend class Transaction;
    template<typename> friend class db::ConnectPool;
};

Transaction::Ptr createTransact(const DriverPtr&);
QSqlResult* createResult(const DriverPtr&);
QSqlResult* createResult(const Transaction::Ptr&);

} // namespace postgres
} // namespace db
