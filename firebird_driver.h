/****************************************************************************
** Modified:
**   2019 Pavel Karelin (hkarel), <hkarel@yandex.ru>
**
** Copyright (C) 2014 Digia Plc and/or its subsidiary(-ies).
** Contact: http://www.qt-project.org/legal
**
** This file is part of the QtSql module of the Qt Toolkit.
**
** $QT_BEGIN_LICENSE:LGPL$
** Commercial License Usage
** Licensees holding valid commercial Qt licenses may use this file in
** accordance with the commercial license agreement provided with the
** Software or, alternatively, in accordance with the terms contained in
** a written agreement between you and Digia.  For licensing terms and
** conditions see http://qt.digia.com/licensing.  For further information
** use the contact form at http://qt.digia.com/contact-us.
**
** GNU Lesser General Public License Usage
** Alternatively, this file may be used under the terms of the GNU Lesser
** General Public License version 2.1 as published by the Free Software
** Foundation and appearing in the file LICENSE.LGPL included in the
** packaging of this file.  Please review the following information to
** ensure the GNU Lesser General Public License version 2.1 requirements
** will be met: http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html.
**
** In addition, as a special exception, Digia gives you certain additional
** rights.  These rights are described in the Digia Qt LGPL Exception
** version 1.1, included in the file LGPL_EXCEPTION.txt in this package.
**
** GNU General Public License Usage
** Alternatively, this file may be used under the terms of the GNU
** General Public License version 3.0 as published by the Free Software
** Foundation and appearing in the file LICENSE.GPL included in the
** packaging of this file.  Please review the following information to
** ensure the GNU General Public License version 3.0 requirements will be
** met: http://www.gnu.org/copyleft/gpl.html.
**
** $QT_END_LICENSE$**
****************************************************************************/

#pragma once

#include "sqlcachedresult.h"
#include "shared/defmac.h"
#include "shared/clife_base.h"
#include "shared/clife_ptr.h"
#include "shared/qt/quuidex.h"

#include <QSqlError>
#include <QSqlDriver>
#include <QSqlResult>
#include <ibase.h>
#include <atomic>

namespace db {

template<typename> class ConnectPool;

namespace firebird {

class Result;
class Driver;
typedef clife_ptr<Driver> DriverPtr;

class Transaction : public clife_base
{
public:
    typedef clife_ptr<Transaction> Ptr;

    bool begin();
    bool commit();
    bool rollback();
    bool isActive() const;

private:
    Transaction(const DriverPtr&);
    DISABLE_DEFAULT_FUNC(Transaction)
    isc_tr_handle* handle() const {return (isc_tr_handle*)&_trans;}

private:
    DriverPtr _drv;
    isc_tr_handle _trans = {0};

    friend class Result;
    friend class Driver;
    friend Transaction::Ptr createTransact(const DriverPtr&);
};

struct AutoRollbackTransact
{
    Transaction::Ptr transact;
    AutoRollbackTransact(const Transaction::Ptr&);
    ~AutoRollbackTransact();
};

#define FIREBIRD_AUTOROLLBACK_TRANSACT(TRANSACT) \
    db::firebird::AutoRollbackTransact __fb_autorollback_transact__(TRANSACT); \
    (void) __fb_autorollback_transact__;

class Result : public SqlCachedResult
{
public:
    enum class ForwardOnly {No = 0, Yes = 1};

    Result(const DriverPtr&, ForwardOnly);
    Result(const Transaction::Ptr&, ForwardOnly);
    ~Result();

    bool prepare(const QString& query) override;
    bool exec() override;
    QVariant handle() const override;

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
    bool checkError(const char* msg, QSqlError::ErrorType type,
                    ISC_STATUS* status, const char* func, int line);

    bool beginInternalTransact();
    bool commitInternalTransact();
    bool rollbackInternalTransact();
    isc_tr_handle* transact() const;

    QVariant fetchBlob(ISC_QUAD* bId);
    bool writeBlob(XSQLVAR& sqlVar, const QByteArray& ba);

    QVariant fetchArray(XSQLVAR& sqlVar, ISC_QUAD* arr);
    bool writeArray(XSQLVAR& sqlVar, const QList<QVariant>& list);

private:
    DISABLE_DEFAULT_COPY(Result)

    DriverPtr        _drv;
    Transaction::Ptr _externalTransact;
    Transaction::Ptr _internalTransact;
    isc_stmt_handle  _stmt       = {0};
    XSQLDA*          _sqlda      = {0};  // output sqlda
    XSQLDA*          _inda       = {0};  // input parameters
    int              _queryType  = {-1}; // Тип sql-запроса
    QString          _preparedQuery;     // Содержит подготовленный запрос
};

class Driver : public QSqlDriver, public clife_base
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

    Transaction::Ptr createTransact() const;

    QSqlResult* createResult() const override;
    QSqlResult* createResult(const Transaction::Ptr&) const;

    QVariant handle() const override;
    bool hasFeature(DriverFeature) const override;

    QStringList tables(QSql::TableType) const override;
    QSqlRecord  record(const QString& tableName) const override;
    QSqlIndex   primaryIndex(const QString& tableName) const override;

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

#if QT_VERSION >= 0x050000
protected:
    bool subscribeToNotification(const QString& name) override;
    bool unsubscribeFromNotification(const QString& name) override;
    QStringList subscribedToNotifications() const override;
#endif
protected slots:
    bool subscribeToNotificationImplementation(const QString& name);
    bool unsubscribeFromNotificationImplementation(const QString& name);
    QStringList subscribedToNotificationsImplementation() const;

private slots:
    void qHandleEventNotification(void* updatedResultBuffer);

private:
    void setOpen(bool) override;

    bool beginTransaction() override;
    bool commitTransaction() override;
    bool rollbackTransaction() override;

    // Фиктивная функция, необходима для совместимости с Postgres-драйвером
    void setThreadId(pid_t) {}

    isc_db_handle* ibase() const {return (isc_db_handle*)&_ibase;}
    bool checkError(const char* msg, QSqlError::ErrorType type,
                    ISC_STATUS* status, const char* func, int line);
private:
    Q_OBJECT
    Driver();
    DISABLE_DEFAULT_COPY(Driver)

    struct EventBuffer
    {
        ISC_UCHAR* eventBuffer;
        ISC_UCHAR* resultBuffer;
        ISC_LONG   bufferLength;
        ISC_LONG   eventId;

        enum SubscriptState
        {
            Starting,
            Subscribed,
            Finished
        };
        SubscriptState subscriptState;
    };

    isc_db_handle    _ibase = {0};
    QTextCodec*      _textCodec = {0};
    volatile bool    _isOpen = {false};
    std::atomic_bool _operationIsAborted = {false};

    QMap<QString, EventBuffer*> _eventBuffers;

    friend class Result;
    friend class Transaction;
    template<typename> friend class db::ConnectPool;
};

Transaction::Ptr createTransact(const DriverPtr&);
QSqlResult* createResult(const DriverPtr&);
QSqlResult* createResult(const Transaction::Ptr&);

// Блокирует обработку SIGTERM внутри драйвера БД
bool setIgnoreSIGTERM();

} // namespace firebird
} // namespace db
