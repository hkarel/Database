/*****************************************************************************
  The MIT License

  Copyright © 2021 Egorov Vladimir, <egorov.vladimir.n@gmail.com>

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

#include <QSqlError>
#include <QSqlDriver>
#include <QSqlQuery>
#include <QSqlResult>


#include <sqlext.h>
#include <atomic>

namespace db {

template<typename> class ConnectPool;

namespace mssql {

class Result;
class Driver;
typedef clife_ptr<Driver> DriverPtr;

//QSqlField qMakeFieldInfo(const SQLHANDLE stmt, int i, QString* errorMessage);
//QSqlField qMakeFieldInfo(const SQLHANDLE stmt, const DriverPtr&);
//QSqlField qMakeFieldInfo(const Result* result, int i);
//QSqlField qMakeFieldInfo(const SQLHANDLE stmt, int i, QString* errorMessage);

//QString qODBCWarn(const Result*, int* nativeCode);
//void qSqlWarning(const QString& message, const Result*, const char* func, int line);
//void qSqlWarning(const QString& message, const Driver*, const char* func, int line);
//void qSqlWarning(const QString& message, const SQLHANDLE stmt, const char* func, int line);

class Result;
class Driver;
typedef clife_ptr<Driver> DriverPtr;

class Transaction final : public clife_base
{
public:
    typedef clife_ptr<Transaction> Ptr;

    // На данный момент работа с уровнем изоляции транзацкий не используется
    enum class IsolationLevel
    {
        ReadUncommitted,
        ReadCommitted,
        RepeatableRead,
        Snapshot,
        Serializable,
    };

    ~Transaction();

    bool begin(IsolationLevel = IsolationLevel::ReadCommitted);
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

class Result final : public QObject, public SqlCachedResult /*QSqlResult*/
{
public:
    enum class ForwardOnly {No = 0, Yes = 1};

    Result(const DriverPtr&, ForwardOnly);
    Result(const Transaction::Ptr&, ForwardOnly);
    ~Result();

    bool prepare(const QString& query) override;
    bool exec() override;

private slots:
    void abortStatement();

protected:
    bool gotoNext(SqlCachedResult::ValueCache& row, int rowIdx) override;
    bool reset(const QString& query) override;
    int  size() override;
    int  numRowsAffected() override;
    QSqlRecord record() const override;

    // Вспомогательная функция, возвращает количество записей для предварительно
    // подготовленного запроса
    int size2(const DriverPtr&) const;

private:
    void setLastError1(const QString& msg, QSqlError::ErrorType,
                       const char* func, int line);
    void setLastError2(const QString& msg, QSqlError::ErrorType,
                       const char* func, int line, const QString& detail,
                       int code);
    bool checkError(const char* msg, QSqlError::ErrorType, SQLRETURN rc,
                    const char* func, int line);

    // Возвращает TRUE если sql-выражение  является  SELECT-запросом  или если
    // в sql-выражении вызывается хранимая процедура возвращающая набор данных.
    bool isSelectSql() const;

    void cleanup();

    bool beginInternalTransact();
    bool commitInternalTransact();
    bool rollbackInternalTransact();
    quint64 transactId() const;

private:
    Q_OBJECT

    DISABLE_DEFAULT_COPY(Result)

    DriverPtr        _drv;
    Transaction::Ptr _externalTransact;
    Transaction::Ptr _internalTransact;
    SQLHANDLE        _stmt = {nullptr};
    QString          _preparedQuery;    // Содержит подготовленный запрос
    int              _numRowsAffected = {-1};

    friend int resultSize(const QSqlQuery&, const DriverPtr&);
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

    QString formatValue(const QSqlField& field, bool trimStrings) const override;
    QString escapeIdentifier(const QString& identifier, IdentifierType) const override;

    // Функция прерывает выполнение текущей sql-операции. Этой функцией нужно
    // пользоваться  когда  необходимо  экстренно  закрыть  подключение  к БД
    // не дожидаясь окончания выполнения sql-запроса. После того как операция
    // будет  прервана - данным  подключением  уже нельзя  будет пользоваться,
    // его можно будет только закрыть
    void abortOperation();

    // Возвращает TRUE если sql-операция была прервана
    bool operationIsAborted() const;

    // Определяет формат приведения строковых данных (UTF8/UTF16) к типу QString.
    // Если  wideChar = TRUE  будет выполняться приведение из UTF16, в противном
    // случае из UTF8.  По  умолчанию  выполняется  приведение  из  UTF8
    bool wideChar() const {return _wideChar;}
    void setWideChar(bool val) {_wideChar = val;}

signals:
    void abortStatement();

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

    // Установка дополнительных параметров строки подключения
    // bool setConnectionOptions(const QString& connOpts);

private:
    Q_OBJECT
    Driver();
    DISABLE_DEFAULT_COPY(Driver)

    SQLHANDLE _env = {nullptr};
    SQLHANDLE _connect = {nullptr};
    pid_t     _threadId = {0};
    quint64   _transactNumber = {0};
    bool      _wideChar = {false};
    std::atomic_bool _operationIsAborted = {false};

    friend class Result;
    friend class Transaction;
    template<typename> friend class db::ConnectPool;
};

Transaction::Ptr createTransact(const DriverPtr&);
QSqlResult* createResult(const DriverPtr&);
QSqlResult* createResult(const Transaction::Ptr&);

int resultSize(const QSqlQuery&, const DriverPtr&);

} // namespace mssql
} // namespace db
