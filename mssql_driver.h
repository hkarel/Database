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
#include <QSqlDatabase>

#include <atomic>
#include <QCoreApplication>
#include <iostream>
#include <QSqlDatabase>
#include <QSqlQuery>
#include <QSqlError>
#include <sqlext.h>
//#include <SQLNCLI.h>
#include <sql.h>
#include <qvariant.h>
#include <qdatetime.h>
#include <qsqlerror.h>
#include <qsqlfield.h>
#include <qsqlindex.h>

namespace db {

template<typename> class ConnectPool;

namespace mssql {

class Result;
class Driver;
typedef clife_ptr<Driver> DriverPtr;

namespace detail {

QSqlField qMakeFieldInfo(const SQLHANDLE hStmt, int i, QString *errorMessage);
QSqlField qMakeFieldInfo(const SQLHANDLE hStmt, const DriverPtr& p);
QSqlField qMakeFieldInfo(const Result* p, int i );
QSqlField qMakeFieldInfo(const SQLHANDLE hStmt, int i, QString *errorMessage);

//QString qWarnODBCHandle(int handleType, SQLHANDLE handle, int *nativeCode = 0);
//QString qODBCWarn(const SQLHANDLE hStmt, const SQLHANDLE envHandle = 0, const SQLHANDLE pDbC = 0, int *nativeCode = 0);
QString qODBCWarn(const Result* odbc, int *nativeCode);
//void qSqlWarning(const QString& message, const Result* odbc)
//void qSqlWarning(const QString &message, const SQLHANDLE hStmt);
//QString qODBCWarn(const Result* odbc, int *nativeCode);
}

class Result;
class Driver;
typedef clife_ptr<Driver> DriverPtr;

class Transaction final : public clife_base
{
public:
    typedef clife_ptr<Transaction> Ptr;

    enum class WritePolicy
    {
        ReadOnly  = 0,
        ReadWrite = 1
    };

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
    bool endTrans();
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

public slots:
    void abortStatement();

protected:

    /**
      Функция получает на входе буфер в виде строки, в которой содержатся
      данные в формате csv, и выполняет экспорт данных в таблицу.
      table - имя таблицы
      colums - список столбцов csv и таблицы
      buffer - буфер данных
    */
    bool copyInsert(const QString& table, const QList<QString>& columns, const QString& buffer);

    void updateStmtHandleState()
    {
        disconnectCount = disconnectCount > 0 ? disconnectCount : 0;
    }

protected:
    bool gotoNext(SqlCachedResult::ValueCache& row, int rowIdx) override;
    bool reset(const QString& query) override;
    int  size() override;
    int size2(const DriverPtr&) const;
    int  numRowsAffected() override;
    QSqlRecord record() const override;

    void clearValues();

private:
    // Возвращает TRUE если sql-выражение  является  SELECT-запросом  или если
    // в sql-выражении вызывается хранимая процедура возвращающая набор данных.
    bool isSelectSql() const;

    bool checkError(const char* msg, QSqlError::ErrorType type,
                            const SQLRETURN r, const SQLHANDLE hStmt, const char* func, int line);

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
    QByteArray       _stmtName;
    SQLHANDLE hStmt = nullptr;

    QSqlRecord recInfo;
    int disconnectCount = 0;
    QString          _preparedQuery;     // Содержит подготовленный запрос

    friend QSqlField detail::qMakeFieldInfo(const SQLHANDLE hStmt, int i, QString *errorMessage);
    friend QSqlField detail::qMakeFieldInfo(const SQLHANDLE hStmt, const DriverPtr& p);
    friend QSqlField detail::qMakeFieldInfo(const Result* p, int i );
    friend QSqlField detail::qMakeFieldInfo(const SQLHANDLE hStmt, int i, QString *errorMessage);

    friend QString detail::qODBCWarn(const Result* odbc, int *nativeCode);
//    friend void qSqlWarning(const QString& message, const Result* odbc);
//    friend void qSqlWarning(const QString& message, const Result *odbc);
//    friend QString qSqlWarning(const QString& message, const SQLHANDLE hStmt);

    friend int resultSize(const QSqlQuery&, const DriverPtr&);
};

class Driver final : public QSqlDriver, public clife_base
{
signals:
    void abortStatement();
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
    QString escapeIdentifier(const QString& identifier, IdentifierType type) const override;

    // Функция прерывает выполнение текущей sql-операции. Этой функцией нужно
    // пользоваться  когда  необходимо  экстренно  закрыть  подключение  к БД
    // не дожидаясь окончания выполнения sql-запроса. После того как операция
    // будет  прервана - данным  подключением  уже нельзя  будет пользоваться,
    // его можно будет только закрыть.
    void abortOperation(/*const SQLHANDLE hStmt*/);

//    void addStmt(SQLHANDLE);
//    void delStmt(SQLHANDLE);

    // Возвращает TRUE если sql-операция была прервана
    bool operationIsAborted() const;
    bool setConnectionOptions(const QString& connOpts);

    SQLHANDLE hEnv = nullptr;
    SQLHANDLE hDbc = nullptr;

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

    pid_t            _threadId = {0};
    Transaction*     _transactAddr = {0};
    std::atomic_bool _operationIsAborted = {false};

    friend class Result;
    friend class Transaction;
    template<typename> friend class db::ConnectPool;

    enum DefaultCase {Lower, Mixed, Upper, Sensitive};

    int disconnectCount = 0;
    const int datetimePrecision = 19;
    bool unicode = true;
    bool useSchema = false;
    bool isFreeTDSDriver = false;
    bool hasSQLFetchScroll = true;
    bool hasMultiResultSets = false;

    //QList<SQLHANDLE> _resultsList;

    friend QSqlField detail::qMakeFieldInfo(const SQLHANDLE hStmt, int i, QString *errorMessage);
    friend QSqlField detail::qMakeFieldInfo(const SQLHANDLE hStmt, const DriverPtr& p);
    friend QSqlField detail::qMakeFieldInfo(const Result* p, int i );
    friend QSqlField detail::qMakeFieldInfo(const SQLHANDLE hStmt, int i, QString *errorMessage);

    //QMutex _stmtMutex;
};

Transaction::Ptr createTransact(const DriverPtr&);
QSqlResult* createResult(const DriverPtr&);
QSqlResult* createResult(const Transaction::Ptr&);

int resultSize(const QSqlQuery&, const DriverPtr&);

} // namespace postgres
} // namespace db
