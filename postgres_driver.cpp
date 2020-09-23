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

#include "postgres_driver.h"

#include "shared/break_point.h"
#include "shared/prog_abort.h"
#include "shared/safe_singleton.h"
#include "shared/logger/logger.h"
#include "shared/logger/format.h"
#include "shared/qt/logger_operators.h"
#include "shared/qt/quuidex.h"
#include "shared/thread/thread_utils.h"

#include <QDateTime>
#include <QRegExp>
#include <QVariant>
#include <QSqlField>
#include <QSqlIndex>
#include <QSqlQuery>
#include <QVarLengthArray>
#include <cstdlib>
#include <utility>
#include <functional>
#include <byteswap.h>

#define log_error_m   alog::logger().error  (__FILE__, __func__, __LINE__, "PostgresDrv")
#define log_warn_m    alog::logger().warn   (__FILE__, __func__, __LINE__, "PostgresDrv")
#define log_info_m    alog::logger().info   (__FILE__, __func__, __LINE__, "PostgresDrv")
#define log_verbose_m alog::logger().verbose(__FILE__, __func__, __LINE__, "PostgresDrv")
#define log_debug_m   alog::logger().debug  (__FILE__, __func__, __LINE__, "PostgresDrv")
#define log_debug2_m  alog::logger().debug2 (__FILE__, __func__, __LINE__, "PostgresDrv")

#define PG_TYPE_BOOL        16   // QBOOLOID
#define PG_TYPE_INT8        18   // OINT1OID
#define PG_TYPE_INT16       21   // QINT2OID
#define PG_TYPE_INT32       23   // QINT4OID
#define PG_TYPE_INT64       20   // QINT8OID

#define PG_TYPE_BYTEARRAY   17   // QBYTEARRAY (BINARY)
#define PG_TYPE_STRING      25   // STRING (NOT BIN)

#define PG_TYPE_FLOAT       700  // QFLOAT4OID
#define PG_TYPE_DOUBLE      701  // QFLOAT8OID

#define PG_TYPE_DATE        1082 // QDATEOID
#define PG_TYPE_TIME        1083 // QTIMEOID
#define PG_TYPE_TIMESTAMP   1114 // QTIMESTAMPOID

#define PG_TYPE_UUID        2950
#define PG_TYPE_UUID_ARRAY  2951
#define PG_TYPE_INT4_ARRAY  1007

namespace db {
namespace postgres {

namespace {

inline quint64 addrToNumber(void* addr)
{
    return reinterpret_cast<QIntegerForSizeof<void*>::Unsigned>(addr);
}

inline PGresultPtr pqexec(PGconn* connect, const char* cmd)
{
    return PGresultPtr(PQexec(connect, cmd));
}

inline ExecStatusType pqexecStatus(const PGresultPtr& result)
{
    return (result) ? PQresultStatus(result) : PGRES_BAD_RESPONSE;
}

QByteArray genStmtName()
{
    static std::atomic_int a {1};
    int i = a++;
    char buff[20] = {0};
    snprintf(buff, sizeof(buff) - 1, "stmt%04d", i);
    return QByteArray(buff);
}

QVariant::Type qPostgresTypeName(int pgType)
{
    switch (pgType)
    {
        case PG_TYPE_BOOL:
            return QVariant::Bool;

        case PG_TYPE_INT8:
        case PG_TYPE_INT16:
        case PG_TYPE_INT32:
            return QVariant::Int;

        case PG_TYPE_INT64:
            return QVariant::LongLong;

        case PG_TYPE_FLOAT:
            return QVariant::Type(qMetaTypeId<float>());

        case PG_TYPE_DOUBLE:
            return QVariant::Double;

        case PG_TYPE_DATE:
            return QVariant::Date;

        case PG_TYPE_TIME:
            return QVariant::Time;

        case PG_TYPE_TIMESTAMP:
            return QVariant::DateTime;

        case PG_TYPE_BYTEARRAY:
            return QVariant::ByteArray;

        case PG_TYPE_STRING:
            return QVariant::String;

        case PG_TYPE_UUID:
            return QVariant::Type(qMetaTypeId<QUuidEx>());

        case PG_TYPE_INT4_ARRAY:
            return QVariant::Type(qMetaTypeId<QVector<qint32>>());

        case PG_TYPE_UUID_ARRAY:
            return QVariant::Type(qMetaTypeId<QVector<QUuidEx>>());

        default:
            return QVariant::Invalid;
    }
}

inline QDate baseDate() {return {2000, 01, 01};}

qint64 toTimeStamp(const QDateTime& dt)
{
    static const qint64 baseMSecs {QDateTime{baseDate()}.toMSecsSinceEpoch()};
    return (dt.toMSecsSinceEpoch() - baseMSecs) * 1000;
}

QDateTime fromTimeStamp(qint64 ts)
{
    static const QDateTime basedate {baseDate()};
    return basedate.addMSecs(ts / 1000);
}

qint64 toTime(const QTime& t)
{
    static const QTime midnight {0, 0, 0, 0};
    return qint64(midnight.msecsTo(t)) * 1000;
}

QTime fromTime(qint64 pgtime)
{
    static const QTime midnight {0, 0, 0, 0};
    return midnight.addMSecs(int(pgtime / 1000));
}

qint32 toDate(const QDate& d)
{
    static const QDate basedate {baseDate()};
    return basedate.daysTo(d);
}

QDate fromDate(qint32 pgdate)
{
    static const QDate basedate {baseDate()};
    return basedate.addDays(pgdate);
}

struct QueryParams
{
    int    nparams      = {0};
    char** paramValues  = {0};
    int*   paramLengths = {0};
    int*   paramFormats = {0};

    QueryParams() = default;
    DISABLE_DEFAULT_COPY(QueryParams)

    ~QueryParams()
    {
        for (int i = 0; i < nparams; ++i)
            free(paramValues[i]);

        delete [] paramValues;
        delete [] paramLengths;
        delete [] paramFormats;
    }

    void init(int nparams)
    {
        this->nparams = nparams;
        paramValues  = new char* [nparams];
        paramLengths = new int   [nparams];
        paramFormats = new int   [nparams];
        for (int i = 0; i < nparams; ++i)
        {
            paramValues [i] = 0;
            paramLengths[i] = 0;
            paramFormats[i] = 1;
        }
    }
};

template<typename T>
using ArrayFillingFunc = std::function<void (qint32* /*ptrArray*/, QVector<T>& /*array*/)>;

template<typename T>
bool getArray(const PGresultPtr& pgres, qint32 fieldType, const char* fieldTypeName,
              qint32 fieldIndex, ArrayFillingFunc<T> fillingFunc, QVector<T>& array /*out*/)
{
    const char* valueBuff = PQgetvalue(pgres, 0, fieldIndex);
    qint32* pArray = (qint32*) valueBuff;

    // Считываем базовые поля заголовка: ndim, ign, elemtype.
    qint32 ndim     = bswap_32(*pArray++); // Мерность массива
    qint32 ign      = bswap_32(*pArray++); // offset for data, removed by libpq
    qint32 elemtype = bswap_32(*pArray++); // Тип PG
    (void) ign;

    // Для пустого массива ndim равен 0. Это верно для любой размерности массива.
    if (ndim == 0)
    {
        array.clear();
        return true;
    }
    if (ndim > 1)
    {
        log_error_m << "Driver support only one-dimension arrays"
                    << ". Field index: " << fieldIndex;
        return false;
    }

    // Считываем дополнительные поля заголовка
    qint32 size  = bswap_32(*pArray++); // Количество элементов в массиве
    qint32 index = bswap_32(*pArray++); // Индекс первого элемента массива ??
    (void) index;

    if (elemtype != fieldType /*PG_TYPE_INT32*/)
    {
        log_error_m << "Type of array not " << fieldTypeName // "PG_TYPE_INT32"
                    << ". Field index: " << fieldIndex;
        return false;
    }

    // Контрольная проверка размера массива
    int len = PQgetlength(pgres, 0, fieldIndex);
    int arraySize = (len - 5 * sizeof(qint32)) / (sizeof(qint32) + sizeof(T));
    if (arraySize != size)
    {
        break_point

        log_error_m << "Size of array incorrect"
                    << ". Field index: " << fieldIndex;
        return false;
    }

    // Считываем массив данных
    array.resize(size);
    fillingFunc(pArray, array);

    return true;
}

template<typename T>
bool setArray(qint32 paramType, const char* paramTypeName, qint32 paramIndex,
              const QVariant& value, ArrayFillingFunc<T> fillingFunc, QueryParams& params)
{
    typedef QVector<T> ArrayType;

    if (!value.canConvert<ArrayType>())
    {
        log_error_m << log_format("Query param%? can't convert to Vector<%?> type",
                                  paramIndex, paramTypeName);
        return false;
    }

    ArrayType array = value.value<ArrayType>();

    int sz = 3 * sizeof(qint32);
    if (!array.empty())
    {
        //   размер заголовка     размер массива данных
        sz = 5 * sizeof(qint32) + array.count() * (sizeof(quint32) + sizeof(T));
    }

    params.paramValues[paramIndex] = (char*)malloc(sz);
    params.paramLengths[paramIndex] = sz;

    qint32* pArray = (qint32*)params.paramValues[paramIndex];

    qint32 ndim = (array.empty()) ? 0 : 1; // Размерность массива
    qint32 ign = 0;                        // ?
    qint32 elemtype = paramType;           // Тип PG
    qint32 size = array.count();           // Длина массива
    qint32 index = 0;                      // Индекс первого элемента массива

    // Записываем базовые поля заголовка: ndim, ign, elemtype.
    *pArray++ = bswap_32(ndim);
    *pArray++ = bswap_32(ign);
    *pArray++ = bswap_32(elemtype);

    if (!array.empty())
    {
        // Записываем дополнительные поля заголовка
        *pArray++ = bswap_32(size);
        *pArray++ = bswap_32(index);

        // Записываем массив данных
        fillingFunc(pArray, array);
    }
    return true;
}

} // namespace

//------------------------------- Transaction --------------------------------

Transaction::~Transaction()
{
    log_debug2_m << "Transaction dtor. Address: " << addrToNumber(this);
    if (isActive())
        rollback();
    _drv->releaseTransactAddr(this);
}

Transaction::Transaction(const DriverPtr& drv) : _drv(drv)
{
    log_debug2_m << "Transaction ctor. Address: " << addrToNumber(this);
    Q_ASSERT(_drv.get());
    _drv->captureTransactAddr(this);
}

bool Transaction::begin(IsolationLevel isolationLevel, WritePolicy writePolicy)
{
    pid_t threadId = trd::gettid();
    if (_drv->threadId() != threadId)
    {
        log_error_m << "Failed begin transaction, threads identifiers not match"
                    << ". Connection thread id: " << _drv->threadId()
                    << ", current thread id: " << threadId;
        return false;
    }
    if (!_drv->transactAddrIsEqual(this))
    {
        log_error_m << "Failed begin transaction, transaction not captured";
        return false;
    }
    if (_drv->operationIsAborted())
    {
        log_error_m << "Failed begin transaction, sql-operation aborted"
                    << ". Connect: " << addrToNumber(_drv->_connect);
        return false;
    }
    if (!_drv->isOpen() || _drv->isOpenError())
    {
        log_error_m << "Failed begin transaction, database not open";
        return false;
    }
    if (_isActive)
    {
        log_error_m << "Transaction already begun: "
                    << addrToNumber(_drv->_connect) << "/" << _transactId;
        return false;
    }

    const char* beginCmd = "BEGIN";

    if (isolationLevel == IsolationLevel::ReadCommitted
        && writePolicy == WritePolicy::ReadOnly)
    {
        beginCmd = "BEGIN READ ONLY";
    }
    else if (isolationLevel == IsolationLevel::RepeatableRead
             && writePolicy == WritePolicy::ReadWrite)
    {
        beginCmd = "BEGIN ISOLATION LEVEL REPEATABLE READ";
    }
    else if (isolationLevel == IsolationLevel::RepeatableRead
             && writePolicy == WritePolicy::ReadOnly)
    {
        beginCmd = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";
    }
    else if (isolationLevel == IsolationLevel::Serializable
             && writePolicy == WritePolicy::ReadWrite)
    {
        beginCmd = "BEGIN ISOLATION LEVEL SERIALIZABLE";
    }
    else if (isolationLevel == IsolationLevel::Serializable
             && writePolicy == WritePolicy::ReadOnly)
    {
        beginCmd = "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY";
    }

    PGresultPtr pgres = pqexec(_drv->_connect, beginCmd);
    ExecStatusType status = pqexecStatus(pgres);
    if (status != PGRES_COMMAND_OK)
    {
        const char* err = PQerrorMessage(_drv->_connect);
        log_error_m << "Failed begin transaction"
                    << ". Connect: " << addrToNumber(_drv->_connect)
                    << ". Detail: " << err;

        // Прерываем использование данного подключения
        _drv->abortOperation();
        return false;
    }

    pgres = pqexec(_drv->_connect, "SELECT txid_current()");
    status = pqexecStatus(pgres);
    if (status != PGRES_TUPLES_OK)
    {
        break_point

        const char* err = PQerrorMessage(_drv->_connect);
        log_error_m << "Failed get transaction id"
                    << ". Connect: " << addrToNumber(_drv->_connect)
                    << ". Detail: " << err;

        pgres = pqexec(_drv->_connect, "ROLLBACK");
        status = pqexecStatus(pgres);
        if (status != PGRES_COMMAND_OK)
        {
            err = PQerrorMessage(_drv->_connect);
            log_error_m << "Failed rollback transaction"
                        << ". Connect: " << addrToNumber(_drv->_connect)
                        << ". Detail: " << err;
        }

        // Прерываем использование данного подключения
        _drv->abortOperation();
        return false;
    }

    //int ll = PQbinaryTuples(res);
    //int n = PQnfields(res);
    //Oid pgType = PQparamtype(res, 0);
    //int iii = PQgetlength(res, 0, 0);
    char* val = PQgetvalue(pgres, 0, 0);
    //_transactId = atoi(val);
    _transactId = strtoull(val, nullptr, 10);
    _isActive = true;

    log_debug2_m << "Transaction begin: "
                 << addrToNumber(_drv->_connect) << "/" << _transactId;
    return true;
}

bool Transaction::commit()
{
    pid_t threadId = trd::gettid();
    if (_drv->threadId() != threadId)
    {
        log_error_m << "Failed commit transaction, threads identifiers not match"
                    << ". Connection thread id: " << _drv->threadId()
                    << ", current thread id: " << threadId;
        return false;
    }
    if (!_drv->transactAddrIsEqual(this))
    {
        log_error_m << "Failed commit transaction, transaction not captured";
        return false;
    }
    if (_drv->operationIsAborted())
    {
        log_error_m << "Failed commit transaction, sql-operation aborted"
                    << ". Connect: " << addrToNumber(_drv->_connect);
        return false;
    }
    if (!_drv->isOpen() || _drv->isOpenError())
    {
        log_error_m << "Failed commit transaction, database not open";
        return false;
    }
    if (!_isActive)
    {
        log_error_m << "Failed commit transaction, transaction not begun"
                    << ". Connect: " << addrToNumber(_drv->_connect);
        return false;
    }

    PGresultPtr pgres = pqexec(_drv->_connect, "COMMIT");
    ExecStatusType status = pqexecStatus(pgres);
    if (status != PGRES_COMMAND_OK)
    {
        const char* err = PQerrorMessage(_drv->_connect);
        log_error_m << "Failed commit transaction: "
                    << addrToNumber(_drv->_connect) << "/" << _transactId
                    << ". Detail: " << err;

        _isActive = false;
        _transactId = -1;
        return false;
    }
    log_debug2_m << "Transaction commit: "
                 << addrToNumber(_drv->_connect) << "/" << _transactId;

    _isActive = false;
    _transactId = -1;
    return true;
}

bool Transaction::rollback()
{
    pid_t threadId = trd::gettid();
    if (_drv->threadId() != threadId)
    {
        log_error_m << "Failed rollback transaction, threads identifiers not match"
                    << ". Connection thread id: " << _drv->threadId()
                    << ", current thread id: " << threadId;
        return false;
    }
    if (!_drv->transactAddrIsEqual(this))
    {
        log_error_m << "Failed rollback transaction, transaction not captured";
        return false;
    }
    if (!_drv->isOpen() || _drv->isOpenError())
    {
        log_error_m << "Failed rollback transaction, database not open";
        return false;
    }
    if (!_isActive)
    {
        log_error_m << "Failed rollback transaction, transaction not begun"
                    << ". Connect: " << addrToNumber(_drv->_connect);
        return false;
    }

    PGresultPtr result = pqexec(_drv->_connect, "ROLLBACK");
    ExecStatusType status = pqexecStatus(result);
    if (status != PGRES_COMMAND_OK)
    {
        const char* err = PQerrorMessage(_drv->_connect);
        log_error_m << "Failed rollback transaction: "
                    << addrToNumber(_drv->_connect) << "/" << _transactId
                    << ". Detail: " << err;

        _isActive = false;
        _transactId = -1;
        return false;
    }
    log_debug2_m << "Transaction rollback: "
                 << addrToNumber(_drv->_connect) << "/" << _transactId;

    _isActive = false;
    _transactId = -1;
    return true;
}

bool Transaction::isActive() const
{
    return _isActive;
}

//---------------------------------- Result ----------------------------------

#define CHECK_ERROR(MSG, ERR_TYPE) \
    checkError(MSG, ERR_TYPE, pgres, __func__, __LINE__)

#define SET_LAST_ERROR(MSG, ERR_TYPE) { \
    setLastError(QSqlError("PostgresResult", MSG, ERR_TYPE, "1")); \
    alog::logger().error(__FILE__, __func__, __LINE__, "PostgresDrv") << MSG; \
}

#define PGR(CMD) PGresultPtr{CMD}

Result::Result(const DriverPtr& drv, ForwardOnly forwardOnly)
    : SqlCachedResult(drv.get()),
      _drv(drv)
{
    Q_ASSERT(_drv.get());
    setForwardOnly(forwardOnly == ForwardOnly::Yes);
}

Result::Result(const Transaction::Ptr& trans, ForwardOnly forwardOnly)
    : SqlCachedResult(trans->_drv.get()),
      _drv(trans->_drv),
      _externalTransact(trans)
{
    Q_ASSERT(_drv.get());
    setForwardOnly(forwardOnly == ForwardOnly::Yes);
}

Result::~Result()
{
    cleanup();
}

bool Result::isSelectSql() const
{
    return isSelect();
}

bool Result::checkError(const char* msg, QSqlError::ErrorType type,
                        const PGresult* result, const char* func, int line)
{
    int status = PQresultStatus(result);
    if (status == PGRES_FATAL_ERROR)
    {
        const char* err = PQerrorMessage(_drv->_connect);
        setLastError(QSqlError("PostgresResult", msg, type, "1"));
        alog::logger().error(__FILE__, func, line, "PostgresDrv") << msg
            << ". Transact: " << addrToNumber(_drv->_connect) << "/" << transactId()
            << ". Detail: "   << err;
        return true;
    }
    return false;
}

void Result::cleanup()
{
    log_debug2_m << "Begin dataset cleanup. Connect: " << addrToNumber(_drv->_connect);

    if (!_externalTransact)
        if (_internalTransact && _internalTransact->isActive())
        {
             if (isSelectSql())
                 rollbackInternalTransact();
             else
                 commitInternalTransact();
        }

    _stmt.reset();
    if (!_stmtName.isEmpty())
    {
        QByteArray sql = "DEALLOCATE " + _stmtName;
        PGresultPtr pgres = pqexec(_drv->_connect, sql);
        QByteArray msg = "Failed deallocate statement " + _stmtName;
        CHECK_ERROR(msg, QSqlError::StatementError);
    }

    _stmtName.clear();
    _preparedQuery.clear();
    SqlCachedResult::cleanup();

    log_debug2_m << "End dataset cleanup. Connect: " << addrToNumber(_drv->_connect);
}

bool Result::beginInternalTransact()
{
    if (_externalTransact)
        return true;

    if (_internalTransact && _internalTransact->isActive())
    {
        log_debug2_m << "Internal transaction already begun";
        return true;
    }

    if (_internalTransact.empty())
        _internalTransact = createTransact(_drv);

    if (!_internalTransact->begin())
    {
        // Детали сообщения об ошибке пишутся в лог внутри метода begin()
        SET_LAST_ERROR("Failed begin internal transaction", QSqlError::TransactionError)
        return false;
    }
    log_debug2_m << "Internal transaction begin";
    return true;
}

bool Result::commitInternalTransact()
{
    if (_externalTransact)
        return true;

    if (!_internalTransact)
    {
        log_error_m << "Failed commit internal transaction"
                    << ". Detail: Internal transaction not created";
        return false;
    }
    if (!_internalTransact->isActive())
    {
        log_error_m << "Failed commit internal transaction"
                    << ". Detail: Internal transaction not begun";
        return false;
    }

    if (!_internalTransact->commit())
    {
        // Детали сообщения об ошибке пишутся в лог внутри метода commit()
        SET_LAST_ERROR("Failed commit internal transaction", QSqlError::TransactionError)
        return false;
    }
    log_debug2_m << "Internal transaction commit";
    return true;
}

bool Result::rollbackInternalTransact()
{
    if (_externalTransact)
        return true;

    if (!_internalTransact)
    {
        log_error_m << "Failed rollback internal transaction"
                    << ". Detail: Internal transaction not created";
        return false;
    }
    if (!_internalTransact->isActive())
    {
        log_error_m << "Failed rollback internal transaction"
                    << ". Detail: Internal transaction not begun";
        return false;
    }

    if (!_internalTransact->rollback())
    {
        // Детали сообщения об ошибке пишутся в лог внутри метода rollback()
        SET_LAST_ERROR("Failed rollback internal transaction", QSqlError::TransactionError)
        return false;
    }
    log_debug2_m << "Internal transaction rollback";
    return true;
}

quint64 Result::transactId() const
{
    if (_externalTransact)
        return _externalTransact->transactId();

    if (_internalTransact)
        return _internalTransact->transactId();

    return 0;
}

bool Result::prepare(const QString& query)
{
    pid_t threadId = trd::gettid();
    if (_drv->threadId() != threadId)
    {
        log_error_m << "Failed prepare query, threads identifiers not match"
                    << ". Connection thread id: " << _drv->threadId()
                    << ", current thread id: " << threadId;
        return false;
    }
    if (_drv->operationIsAborted())
    {
        SET_LAST_ERROR("Sql-operation aborted", QSqlError::UnknownError)
        return false;
    }
    if (!_drv->isOpen() || _drv->isOpenError())
    {
        SET_LAST_ERROR("Database not open", QSqlError::ConnectionError)
        return false;
    }

    QString pgQuery;
    pgQuery.reserve(query.length() * 1.2);

    int ind = 1;
    for (QChar ch : query)
    {
        if (ch == '?')
        {
            pgQuery += QChar('$');
            pgQuery += QString::number(ind++);
        }
        else
            pgQuery += ch;
    }

    cleanup();
    setActive(false);
    setAt(QSql::BeforeFirstRow);

    if (!beginInternalTransact())
        return false;

    if (alog::logger().level() == alog::Level::Debug2)
    {
        QString sql = pgQuery;
        static QRegExp reg {R"(\s{2,})"};
        sql.replace(reg, " ");
        sql.replace(" ,", ",");
        if (!sql.isEmpty() && (sql[0] == QChar(' ')))
            sql.remove(0, 1);
        log_debug2_m << "Begin prepare query"
                     << ". Transact: " << addrToNumber(_drv->_connect) << "/" << transactId()
                     << ". " << sql;
    }

    PGresultPtr pgres;
    QByteArray stmtName = genStmtName();

    pgres = PGR(PQprepare(_drv->_connect, stmtName, pgQuery.toUtf8(), 0, nullptr));
    if (CHECK_ERROR("Could not prepare statement", QSqlError::StatementError))
    {
        rollbackInternalTransact();
        return false;
    }
    _stmtName = stmtName;

    pgres = PGR(PQdescribePrepared(_drv->_connect, _stmtName));
    if (CHECK_ERROR("Could not get describe for prepared statement", QSqlError::StatementError))
    {
        rollbackInternalTransact();
        return false;
    }
    _stmt = pgres;

    // nfields - число столбцов (полей) в каждой строке полученной выборки.
    // При выполнении INSERT или UPDATE запроса, столбцы не выбираются, поэтому
    // предполагаем количество таких столбцов будет равно 0. В этом случае запрос
    // будет установлен как "Not Select".
    int nfields = PQnfields(_stmt);
    setSelect(nfields != 0);

//    if (nfields == 0)
//    {
//        break_point
//        // отладить
//    }

    _preparedQuery = pgQuery;

    log_debug2_m << "End prepare query"
                 << ". Transact: " << addrToNumber(_drv->_connect) << "/" << transactId();
    return true;
}

bool Result::exec()
{
    pid_t threadId = trd::gettid();
    if (_drv->threadId() != threadId)
    {
        log_error_m << "Failed exec query, threads identifiers not match"
                    << ". Connection thread id: " << _drv->threadId()
                    << ", current thread id: " << threadId;
        return false;
    }
    if (_drv->operationIsAborted())
    {
        SET_LAST_ERROR("Sql-operation aborted", QSqlError::UnknownError)
        return false;
    }
    if (!_drv->isOpen() || _drv->isOpenError())
    {
        SET_LAST_ERROR("Database not open", QSqlError::ConnectionError)
        return false;
    }

    if (!beginInternalTransact())
        return false;

    log_debug2_m << "Start exec query"
                 << ". Transact: " << addrToNumber(_drv->_connect) << "/" << transactId();

    setActive(false);
    setAt(QSql::BeforeFirstRow);

    QueryParams params;
    int nparams = PQnparams(_stmt);

    if (nparams != 0)
    {
        params.init(nparams);
        const QVector<QVariant>& values = boundValues();

        if (alog::logger().level() == alog::Level::Debug2)
        {
            for (int i = 0; i < values.count(); ++i)
                log_debug2_m << "Query param" << i << ": " << values[i];
        }
        if (values.count() != nparams)
        {
            QString msg = "Parameter mismatch, expected %1, got %2 parameters"
                          ". Transact: %3/%4";
            msg = msg.arg(nparams)
                     .arg(values.count())
                     .arg(addrToNumber(_drv->_connect))
                     .arg(transactId());
            SET_LAST_ERROR(msg, QSqlError::StatementError)
            rollbackInternalTransact();
            return false;
        }

        for (int i = 0; i < nparams; ++i)
        {
            const QVariant& val = values[i];

            if (val.isNull())
                continue;

            if (!val.isValid())
            {
                QString msg = "Query param%1 is invalid. Transact: %2/%3";
                msg = msg.arg(i)
                         .arg(addrToNumber(_drv->_connect))
                         .arg(transactId());
                SET_LAST_ERROR(msg, QSqlError::StatementError)
                rollbackInternalTransact();
                return false;
            }
            else if (val.userType() == qMetaTypeId<QUuidEx>())
            {
                const QUuidEx& uuid = val.value<QUuidEx>();
                if (uuid.isNull())
                    continue;
            }
            else if (val.userType() == qMetaTypeId<QUuid>())
            {
                const QUuid& uuid = val.value<QUuid>();
                if (uuid.isNull())
                    continue;
            }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"

            int paramtype = PQparamtype(_stmt, i);
            switch (paramtype)
            {
                case PG_TYPE_BOOL:
                {
                    bool v = val.toBool();
                    int sz = 1;
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    *(params.paramValues[i]) = v;
                    break;
                }
                case PG_TYPE_INT8:
                {
                    qint8 v = val.toInt();
                    int sz = sizeof(v);
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    *((qint8*)params.paramValues[i]) = v;
                    break;
                }
                case PG_TYPE_INT16:
                {
                    qint16 v = val.toInt();
                    int sz = sizeof(v);
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    *((qint16*)params.paramValues[i]) = bswap_16(v);
                    break;
                }
                case PG_TYPE_INT32:
                {
                    qint32 v = val.toInt();
                    int sz = sizeof(v);
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    *((qint32*)params.paramValues[i]) = bswap_32(v);
                    break;
                }
                case PG_TYPE_INT64:
                {
                    qint64 v = val.toLongLong();
                    int sz = sizeof(v);
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    *((qint64*)params.paramValues[i]) = bswap_64(v);
                    break;
                }
                case PG_TYPE_BYTEARRAY:
                {
                    QByteArray v = val.toByteArray();
                    int sz = v.length();
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    memcpy(params.paramValues[i], v.constData(), sz);
                    break;
                }
                case PG_TYPE_STRING:
                {
                    QByteArray v = val.toString().toUtf8();
                    int sz = v.length();
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    memcpy(params.paramValues[i], v.constData(), sz);
                    break;
                }
                case PG_TYPE_FLOAT:
                {
                    float v = val.toDouble();
                    int sz = sizeof(v);
                    qint32 v2 = *((qint32*) &v);
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    *((qint32*)params.paramValues[i]) = bswap_32(v2);
                    break;
                }
                case PG_TYPE_DOUBLE:
                {
                    double v = val.toDouble();
                    int sz = sizeof(v);
                    qint64 v2 = *((qint64*) &v);
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    *((qint64*)params.paramValues[i]) = bswap_64(v2);
                    break;
                }
                case PG_TYPE_DATE:
                {
                    qint32 v = toDate(val.toDate());
                    int sz = sizeof(v);
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    *((qint32*)params.paramValues[i]) = bswap_32(v);
                    break;
                }
                case PG_TYPE_TIME:
                {
                    qint64 v = toTime(val.toTime());
                    int sz = sizeof(v);
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    *((qint64*)params.paramValues[i]) = bswap_64(v);
                    break;
                }
                case PG_TYPE_TIMESTAMP:
                {
                    qint64 v = toTimeStamp(val.toDateTime());
                    int sz = sizeof(v);
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    *((qint64*)params.paramValues[i]) = bswap_64(v);
                    break;
                }
                case PG_TYPE_UUID:
                {
                    QByteArray v;
                    if (val.userType() == qMetaTypeId<QUuidEx>())
                    {
                        const QUuidEx& uuid = val.value<QUuidEx>();
                        v = uuid.toRfc4122();
                    }
                    else if (val.userType() == qMetaTypeId<QUuid>())
                    {
                        const QUuid& uuid = val.value<QUuid>();
                        v = uuid.toRfc4122();
                    }
                    else
                    {
                        QString msg = "Query param%1 is not UUID type. Transact: %2/%3";
                        msg = msg.arg(i)
                                 .arg(addrToNumber(_drv->_connect))
                                 .arg(transactId());
                        SET_LAST_ERROR(msg, QSqlError::StatementError)
                        rollbackInternalTransact();
                        return false;
                    }

                    int sz = v.length();
                    params.paramValues[i] = (char*)malloc(sz);
                    params.paramLengths[i] = sz;
                    memcpy(params.paramValues[i], v.constData(), sz);
                    break;
                }
                case PG_TYPE_INT4_ARRAY:
                {
                    if (!val.canConvert<QVector<qint32>>())
                    {
                        QString msg =
                            "Query param%1 can't convert to Vector<PG_TYPE_INT32> type"
                            ". Transact: %2/%3";
                        msg = msg.arg(i)
                                 .arg(addrToNumber(_drv->_connect))
                                 .arg(transactId());
                        SET_LAST_ERROR(msg, QSqlError::StatementError)
                        rollbackInternalTransact();
                        return false;
                    }

                    auto fillingFunc = [](qint32* ptrArray, QVector<qint32>& array)
                    {
                        for (const qint32 item : array)
                        {
                            *ptrArray++ = bswap_32((qint32)sizeof(qint32));
                            *ptrArray++ = bswap_32(item);
                        }
                    };
                    if (!setArray<qint32>(PG_TYPE_INT32, "PG_TYPE_INT32", i, val, fillingFunc, params))
                    {
                        return false;
                    }
                    break;
                }
                case PG_TYPE_UUID_ARRAY:
                {
                    if (!val.canConvert<QVector<QUuidEx>>())
                    {
                        QString msg =
                            "Query param%1 can't convert to Vector<PG_TYPE_UUID> type"
                            ". Transact: %2/%3";
                        msg = msg.arg(i)
                                 .arg(addrToNumber(_drv->_connect))
                                 .arg(transactId());
                        SET_LAST_ERROR(msg, QSqlError::StatementError)
                        rollbackInternalTransact();
                        return false;
                    }

                    auto fillingFunc = [](qint32* ptrArray, QVector<QUuidEx>& array)
                    {
                        for (const QUuidEx& item : array)
                        {
                            *ptrArray++ = bswap_32((qint32)sizeof(QUuidEx));

                            const QByteArray& ba = item.toRfc4122();
                            memcpy(ptrArray, ba.constData(), 16);
                            ptrArray += 4;
                        }
                    };
                    if (!setArray<QUuidEx>(PG_TYPE_UUID, "PG_TYPE_UUID", i, val, fillingFunc, params))
                    {
                        return false;
                    }
                    break;
                }
                default:
                {
                    QString msg = "Query param%1, is unknown datatype: %2. Transact: %2/%3";
                    msg = msg.arg(i)
                             .arg(paramtype)
                             .arg(addrToNumber(_drv->_connect))
                             .arg(transactId());
                    SET_LAST_ERROR(msg, QSqlError::StatementError)
                    rollbackInternalTransact();
                    return false;
                }
            }

#pragma GCC diagnostic pop

        }
    } // if (nparams != 0)

    if (isSelectSql())
    {
        if (1 != PQsendQueryPrepared(_drv->_connect, _stmtName,
                                     nparams,             // int nParams,
                                     params.paramValues,  // const char * const *paramValues,
                                     params.paramLengths, // const int *paramLengths,
                                     params.paramFormats, // const int *paramFormats,
                                     1 ))                 // int resultFormat
        {
            log_debug2_m <<  PQerrorMessage(_drv->_connect);
            SET_LAST_ERROR("Failed call PQsendQueryPrepared()", QSqlError::UnknownError)
            rollbackInternalTransact();
            return false;
        }
    }
    else
    {
        PGresultPtr pgres;

        pgres = PGR(PQexecPrepared(_drv->_connect, _stmtName,
                                   nparams,             // int nParams,
                                   params.paramValues,  // const char * const *paramValues,
                                   params.paramLengths, // const int *paramLengths,
                                   params.paramFormats, // const int *paramFormats,
                                   1 ));

        if (CHECK_ERROR("Could not prepare statement", QSqlError::StatementError))
        {
            rollbackInternalTransact();
            return false;
        }
    }

    if (isSelectSql())
    {
        if (1 != PQsetSingleRowMode(_drv->_connect))
        {
            SET_LAST_ERROR("Failed turn on single-row mode", QSqlError::UnknownError)
            rollbackInternalTransact();
            return false;
        }
    }

    // После удачного вызова PQsendQueryPrepared() вызовы PQgetResult() буду
    // фетчить данные по одной записи.  Поэтому  вызовы PQgetResult()  нужно
    // выполнять в Result::gotoNext() до тех пор пока  функция  PQgetResult()
    // не вернет NULL

    if (_drv->operationIsAborted())
    {
        SET_LAST_ERROR("Sql-operation aborted", QSqlError::UnknownError)
        rollbackInternalTransact();
        return false;
    }

    if (isSelectSql())
    {
        int nfields = PQnfields(_stmt);
        init(nfields);
    }

    quint64 transId = transactId();

    if (!isSelectSql())
        if (!commitInternalTransact())
        {
            log_debug2_m << "Failed exec query"
                         << ". Transact: " << addrToNumber(_drv->_connect) << "/" << transId;
            return false;
        }

    setActive(true);

    log_debug2_m << "End exec query"
                 << ". Transact: " << addrToNumber(_drv->_connect) << "/" << transId;
    return true;
}

bool Result::copyInsert(const QString& table, const QList<QString>& columns, const QString& buffer)
{
    const char *errmsg = nullptr;

    PGresultPtr pgres;

    pid_t threadId = trd::gettid();
    if (_drv->threadId() != threadId)
    {
        log_error_m << "Failed prepare query, threads identifiers not match"
                    << ". Connection thread id: " << _drv->threadId()
                    << ", current thread id: " << threadId;
        return false;
    }
    if (_drv->operationIsAborted())
    {
        SET_LAST_ERROR("Sql-operation aborted", QSqlError::UnknownError)
        return false;
    }
    if (!_drv->isOpen() || _drv->isOpenError())
    {
        SET_LAST_ERROR("Database not open", QSqlError::ConnectionError)
        return false;
    }

    cleanup();
    setActive(false);
    setAt(QSql::BeforeFirstRow);

    if (!beginInternalTransact())
        return false;

    QString sql = " COPY %1(%2)              "
                  " FROM STDIN DELIMITER ';' "
                  " CSV ENCODING 'UTF8'      "
                  " QUOTE '\"'               "
                  " ESCAPE '''';             ";

    sql = sql.arg(table);

    QString join;
    if (columns.size() == 1)
    {
        join = columns.first();
    }
    else
    {
        join = columns.first();
        QList<QString> cols = columns;
        cols.removeAt(0);
        for (const QString& item : cols)
        {
            join += "," + item;
        }
    }

    sql = sql.arg(join);

    pgres = pqexec(_drv->_connect, sql.toStdString().c_str());

    if (!pgres)
        PQresultErrorMessage(pgres);

    int bufSize = strlen(buffer.toStdString().c_str());

    if (1 != PQputCopyData(_drv->_connect, buffer.toStdString().c_str(), bufSize))
        log_error_m << "Failed call PQputCopyData()"
                    << ". Detail: " << PQerrorMessage(_drv->_connect);

    if (1 != PQputCopyEnd(_drv->_connect, errmsg))
        log_error_m << "Failed call PQputCopyEnd()"
                    << ". Detail: " << PQerrorMessage(_drv->_connect);

    if (errmsg)
        log_error_m << "Failed insert data: " << errmsg
                    << ". Detail: " << PQerrorMessage(_drv->_connect);

    if (!commitInternalTransact())
        return false;

    return  true;
}

bool Result::gotoNext(SqlCachedResult::ValueCache& row, int rowIdx)
{
    if (_drv->operationIsAborted())
    {
        setAt(QSql::AfterLastRow);
        return false;
    }

    PGresultPtr pgres {PQgetResult(_drv->_connect)};
    if (pgres.empty())
    {
        setAt(QSql::AfterLastRow);
        return false;
    }

    int ntuples = PQntuples(pgres);
    int status = PQresultStatus(pgres);
    if ((status == PGRES_TUPLES_OK) && (ntuples == 0))
    {
        setAt(QSql::AfterLastRow);
        return false;
    }
    if (status != PGRES_SINGLE_TUPLE)
    {
        log_error_m << "Tuples data not PGRES_SINGLE_TUPLE";
        CHECK_ERROR("Failed fetch record", QSqlError::StatementError);
        setAt(QSql::AfterLastRow);
        return false;
    }
    if (1 != PQbinaryTuples(pgres))
    {
        log_error_m << "Tuples data must be in binary format";
        setAt(QSql::AfterLastRow);
        return false;
    }

    if (rowIdx < 0) // not interested in actual values
    {
        break_point
        // отладить

        log_warn_m << "Condition happened: rowIdx < 0";
        return true;
    }

    int nfields = PQnfields(_stmt);
    for (int i = 0; i < nfields; ++i)
    {
        int idx = rowIdx + i;
        int ftype = PQftype(_stmt, i);

        if (1 == PQgetisnull(pgres, 0, i))
        {
            QVariant v;
            v.convert(qPostgresTypeName(ftype));
            row[idx] = v;
            continue;
        }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wstrict-aliasing"

        const char* value = PQgetvalue(pgres, 0, i);
        switch (ftype)
        {
            case PG_TYPE_BOOL:
                row[idx] = QVariant(bool(*value));
                break;

            case PG_TYPE_INT8:
                row[idx] = QVariant(qint32(*(qint8*)value));
                break;

            case PG_TYPE_INT16:
                row[idx] = QVariant(qint32(bswap_16(*(qint16*)value)));
                break;

            case PG_TYPE_INT32:
                row[idx] = QVariant(qint32(bswap_32(*(qint32*)value)));
                break;

            case PG_TYPE_INT64:
                row[idx] = QVariant(qint64(bswap_64(*(qint64*)value)));
                break;

            case PG_TYPE_FLOAT:
            {
                qint32 val = bswap_32(*(qint32*)value);
                float f = *((float*) &val);
                row[idx] = QVariant(f);
                break;
            }
            case PG_TYPE_DOUBLE:
            {
                qint64 val = bswap_64(*(qint64*)value);
                double d = *((double*) &val);
                row[idx] = QVariant(d);
                break;
            }
            case PG_TYPE_DATE:
                row[idx] = fromDate(bswap_32(*(qint32*)value));
                break;

            case PG_TYPE_TIME:
                row[idx] = fromTime(bswap_64(*(qint64*)value));
                break;

            case PG_TYPE_TIMESTAMP:
                row[idx] = fromTimeStamp(bswap_64(*(qint64*)value));
                break;

            case PG_TYPE_BYTEARRAY:
            {
                /**
                  PQfsize - возвращает размер поля определенный в БД, для полей
                            переменной длинны возвращается -1.
                  PQgetlength - возвращает текущий размер данных в датасете
                  int fsize = PQfsize(_stmt, i);
                  int len = PQgetlength(pgres, 0, i);
                */
                int len = PQgetlength(pgres, 0, i);
                row[idx] = QByteArray(value, len);
                break;
            }
            case PG_TYPE_STRING:
                row[idx] = QString::fromUtf8(value).trimmed();
                break;

            case PG_TYPE_UUID:
            {
#ifndef NDEBUG
                if (16 != PQfsize(_stmt, i))
                {
                    log_error_m << "Raw uuid field must be 16 bytes"
                                << ". Field index: " << i;
                    //row[idx].setValue(QUuidEx());
                    setAt(QSql::AfterLastRow);
                    return false;
                }
#endif
                const QUuid& uuid =
                    QUuid::fromRfc4122(QByteArray::fromRawData(value, 16));
                const QUuidEx& uuidex = static_cast<const QUuidEx&>(uuid);
                row[idx].setValue(uuidex);
                break;
            }
            case PG_TYPE_INT4_ARRAY:
            {
                QVector<qint32> array;
                auto fillingFunc = [](qint32* ptrArray, QVector<qint32>& array)
                {
                    for (int i = 0; i < array.count(); ++i)
                    {
                        ++ptrArray; // Пропустить размер значения
                        array[i] = bswap_32(*ptrArray++);
                    }
                };
                if (!getArray<qint32>(pgres, PG_TYPE_INT32, "PG_TYPE_INT32", i, fillingFunc, array))
                {
                    setAt(QSql::AfterLastRow);
                    return false;
                }

                row[idx].setValue(array);
                break;
            }
            case PG_TYPE_UUID_ARRAY:
            {
                QVector<QUuidEx> array;
                auto fillingFunc = [](qint32* ptrArray, QVector<QUuidEx>& array)
                {
                    for (int i = 0; i < array.count(); ++i)
                    {
                        ++ptrArray; // Пропустить размер значения

                        const QUuid& uuid =
                            QUuid::fromRfc4122(QByteArray::fromRawData((char*)ptrArray, 16));
                        const QUuidEx& uuidex = static_cast<const QUuidEx&>(uuid);

                        array[i] = uuidex;
                        ptrArray = ptrArray + 4;
                    }
                };
                if (!getArray<QUuidEx>(pgres, PG_TYPE_UUID, "PG_TYPE_UUID", i, fillingFunc, array))
                {
                    setAt(QSql::AfterLastRow);
                    return false;
                }

                row[idx].setValue(array);
                break;
            }
            default:
                row[idx] = QVariant();
        }

#pragma GCC diagnostic pop

    } // for (int i = 0; i < nfields; ++i)

    return true;
}

bool Result::reset(const QString& query)
{
    if (!prepare(query))
        return false;

    return exec();
}

int Result::size()
{
    break_point
    // отладить

    if (!isActive() || !isSelectSql() || _preparedQuery.isEmpty())
    {
        log_error_m << "Size of result unavailable";
        return -1;
    }

    Transaction::Ptr transact =
        (_externalTransact) ? _externalTransact : _internalTransact;

    if (transact.empty())
    {
        log_error_m << "Size of result unavailable"
                       ". Detail: Transaction not created";
        return -1;
    }
    if (!transact->isActive())
    {
        log_error_m << "Size of result unavailable"
                       ". Detail: Transaction not active";
        return -1;
    }

    int pos = _preparedQuery.indexOf("FROM", Qt::CaseInsensitive);
    if (pos == -1)
    {
        log_error_m << "Size of result unavailable"
                       ". Detail: Sql-statement not contains 'FROM' keyword";
        return -1;
    }

    QString query = "SELECT COUNT(*) " + _preparedQuery.mid(pos);

    pos = query.indexOf("ORDER BY", Qt::CaseInsensitive);
    if (pos != -1)
        query.remove(pos, query.length());

    QSqlQuery q {createResult(transact)};

    if (!q.prepare(query))
    {
        log_error_m << "Size of result unavailable";
        return -1;
    }

    const QVector<QVariant>& values = boundValues();
    for (int i = 0; i < values.count(); ++i)
    {
        const QVariant& val = values[i];
        q.addBindValue(val);
    }
    if (!q.exec())
    {
        log_error_m << "Size of result unavailable";
        return -1;
    }

    q.first();
    return q.record().value(0).toInt();
}

int Result::numRowsAffected()
{
    break_point
    // написать реализацию

    return -1;
}

QSqlRecord Result::record() const
{
    QSqlRecord rec;
    if (!isActive() || !isSelectSql())
        return rec;

    int nfields = PQnfields(_stmt);
    for (int i = 0; i < nfields; ++i)
    {
        const char* fname = PQfname(_stmt, i);
        int ftype = PQftype(_stmt, i);

        QVariant::Type fieldType = qPostgresTypeName(ftype);
        if (fieldType == QVariant::Invalid)
        {
            log_error_m << "Unknown field type"
                        << ". Field name: " << fname
                        << ". Oid: " << ftype;
        }
        QSqlField f {QString::fromUtf8(fname).trimmed(), fieldType};

        int fsize = PQfsize(_stmt, i);
        f.setLength(fsize);

        int fmod = PQfmod(_stmt, i);
        f.setPrecision(fmod);

        f.setSqlType(ftype);
        rec.append(f);
    }
    return rec;
}

#undef CHECK_ERROR
#undef SET_LAST_ERROR
#undef PGR

//-------------------------------- Driver ------------------------------------

Driver::Driver() : QSqlDriver(0)
{}

Driver::~Driver()
{
    close();
}

Driver::Ptr Driver::create()
{
    return Driver::Ptr(new Driver);
}

bool Driver::open(const QString& db,
                  const QString& user,
                  const QString& password,
                  const QString& host,
                  int   port,
                  const QString& connOpts)
{
    if (isOpen())
        close();

    bool threadSafety = PQisthreadsafe();
    if (!threadSafety)
    {
        const char* msg = "Library libpq is not thread safe";
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, "1"));

        log_error_m << msg;

        setOpenError(true);
        return false;
    }

    QString connString;
    auto quote = [](QString s) -> QString
    {
        s.replace(QChar('\\'), QLatin1String("\\\\"));
        s.replace(QChar('\''), QLatin1String("\\'"));
        s.append(QChar('\'')).prepend(QChar('\''));
        return s;
    };

    if (!host.isEmpty())
        connString += QString("host=%1").arg(quote(host));

    if (!db.isEmpty())
        connString += QString(" dbname=%1").arg(quote(db));

    if (!user.isEmpty())
        connString += QString(" user=%1").arg(quote(user));

    if (!password.isEmpty())
        connString += QString(" password=%1").arg(quote(password));

    if (port != -1)
        connString += (QString(" port=%1")).arg(quote(QString::number(port)));

    QString options = connOpts.trimmed();
    if (!options.isEmpty())
    {
        QString options_ = options;
        options_.replace(QChar(';'), QChar(' '), Qt::CaseInsensitive);
        connString.append(QChar(' ')).append(options_);
    }

    { //Block for alog::Line
        alog::Line logLine = log_verbose_m << "Try open database '" << db << "'"
                                           << ". User: " << user
                                           << ", host: " << host
                                           << ", port: " << port;
        if (!options.isEmpty())
            logLine << ", options: " << options;
    }

    _connect = PQconnectdb(connString.toUtf8());
    if (PQstatus(_connect) == CONNECTION_BAD)
    {
        const char* msg = "Error opening database";
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, "1"));

        const char* err = PQerrorMessage(_connect);
        log_error_m << msg << "; Detail: " << err;

        setOpenError(true);
        PQfinish(_connect);
        _connect = 0;
        return false;
    }

    int protocolVers = PQprotocolVersion(_connect);
    if (protocolVers < 3)
    {
        const char* msg = "PostgreSQL protocol version must be not less than 3";
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, "1"));

        log_error_m << msg;

        setOpenError(true);
        PQfinish(_connect);
        _connect = 0;
        return false;
    }

    int serverVers = PQserverVersion(_connect) / 10000;
    if (serverVers < 9)
    {
        const char* msg = "PostgreSQL server version must be not less than 9";
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, "1"));

        log_error_m << msg;

        setOpenError(true);
        PQfinish(_connect);
        _connect = 0;
        return false;
    }

/*
    PGresult* result = PQexec(_connection, "SHOW server_version");
    int status = PQresultStatus(result);
    if ((status == PGRES_COMMAND_OK) || (status == PGRES_TUPLES_OK))
    {
        QString verStr = QString::fromLatin1(PQgetvalue(result, 0, 0));
        PQclear(result);

        int verMajor = 0;
        const QRegExp rx {R"(^(\d+)\.(\d+).*)"};
        if (rx.exactMatch(verStr))
            verMajor = rx.cap(1).toInt();

        if (verMajor < 9)
        {
            const char* msg = "PostgreSQL server version must be not less than 9";
            setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, "1"));

            log_error_m << msg;

            setOpenError(true);
            PQfinish(_connection);
            _connection = 0;
            return false;
        }
    }
    else
    {
        PQclear(result);

        const char* msg = "Failed get PostgreSQL server version";
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, "1"));

        const char* err = PQerrorMessage(_connection);
        log_error_m << msg << "; Detail: " << err;

        setOpenError(true);
        PQfinish(_connection);
        _connection = 0;
        return false;
    }
*/

/*
    PGresult* result = PQexec(_connect, "SET CLIENT_ENCODING TO 'UNICODE'");
    int status = PQresultStatus(result);
    PQclear(result);
    if (status != PGRES_COMMAND_OK)
    {
        const char* msg = "Failed set CLIENT_ENCODING to 'UNICODE'";
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, "1"));

        const char* err = PQerrorMessage(_connect);
        log_error_m << msg << "; Detail: " << err;

        setOpenError(true);
        PQfinish(_connect);
        _connect = 0;
        return false;
    }
*/

    //int enc = PQclientEncoding(_connect);
    //const char* encName = pg_encoding_to_char(enc);
    //if (strcmp(encName, "UTF8") != 0)
    if (PQsetClientEncoding(_connect, "UTF8") == -1)
    {
        const char* msg = "Only UTF8 encoding is support";
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, "1"));

        log_error_m << msg;

        setOpenError(true);
        PQfinish(_connect);
        _connect = 0;
        return false;
    }

    _threadId = trd::gettid();

    //int a = PQbeginBatchMode(_connect);

    setOpen(true);
    setOpenError(false);
    log_verbose_m << "Database is open. Connect: " << addrToNumber(_connect);

    return true;
}

bool Driver::open(const QString& db,
                  const QString& user,
                  const QString& password,
                  const QString& host,
                  int   port)
{
    return open(db, user, password, host, port, QString());
}

void Driver::close()
{
    if (!isOpen())
        return;

    PGconn* connect = _connect;
    if (_connect)
        PQfinish(_connect);

    _connect = 0;
    _threadId = 0;
    _transactAddr = 0;

    setOpen(false);
    setOpenError(false);

    log_verbose_m << "Database is closed. Connect: " << addrToNumber(connect);
}

bool Driver::isOpen() const
{
    return (PQstatus(_connect) == CONNECTION_OK);
}

void Driver::setOpen(bool val)
{
    QSqlDriver::setOpen(val);
}

Transaction::Ptr Driver::createTransact() const
{
    return Transaction::Ptr(new Transaction(Driver::Ptr((Driver*)this)));
}

QSqlResult* Driver::createResult() const
{
    return new Result(Driver::Ptr((Driver*)this), Result::ForwardOnly::Yes);
    return 0;
}

QSqlResult* Driver::createResult(const Transaction::Ptr& transact) const
{
    return new Result(transact, Result::ForwardOnly::Yes);
    return 0;
}

bool Driver::hasFeature(DriverFeature f) const
{
    switch (f)
    {
#if QT_VERSION >= 0x050000
        case CancelQuery:
#endif
        case NamedPlaceholders:
        case LastInsertId:
        case BatchOperations:
        case SimpleLocking:
        case FinishQuery:
        case MultipleResultSets:
            return false;

        case QuerySize:
        case Transactions:
        case PreparedQueries:
        case PositionalPlaceholders:
        case Unicode:
        case BLOB:
        case EventNotifications:
        case LowPrecisionNumbers:
            return true;
    }
    return false;
}

bool Driver::beginTransaction()
{
    log_debug2_m << "Call beginTransaction()";
    return false;
}

bool Driver::commitTransaction()
{
    log_debug2_m << "Call commitTransaction()";
    return false;
}

bool Driver::rollbackTransaction()
{
    log_debug2_m << "Call rollbackTransaction()";
    return false;
}

void Driver::captureTransactAddr(Transaction* transact)
{
    if (_transactAddr == 0)
    {
        _transactAddr = transact;
        log_debug2_m << "Transaction address captured: " << addrToNumber(transact)
                     << ". Connect: " << addrToNumber(_connect);
    }
    else
        log_warn_m << "Failed capture transaction address: "  << addrToNumber(transact)
                   << ". Already captured: " << addrToNumber(_transactAddr)
                   << ". Connect: " << addrToNumber(_connect);
}

void Driver::releaseTransactAddr(Transaction* transact)
{
    if (_transactAddr == transact)
    {
        _transactAddr = 0;
        log_debug2_m << "Transaction address released: " << addrToNumber(transact)
                     << ". Connect: " << addrToNumber(_connect);
    }
    else
        log_warn_m << "Failed release transaction address: "  << addrToNumber(transact)
                   << ". Already captured: " << addrToNumber(_transactAddr)
                   << ". Connect: " << addrToNumber(_connect);
}

bool Driver::transactAddrIsEqual(Transaction* transact)
{
    return (_transactAddr == transact);
}

//QStringList Driver::tables(QSql::TableType type) const
//{
//}

//QSqlRecord Driver::record(const QString& tableName) const
//{
//}

//QSqlIndex Driver::primaryIndex(const QString& table) const
//{
//}

QString Driver::formatValue(const QSqlField& field, bool trimStrings) const
{
    return QSqlDriver::formatValue(field, trimStrings);
}

//#if QT_VERSION >= 0x050000
//bool Driver::subscribeToNotification(const QString& name)
//{
//    return subscribeToNotificationImplementation(name);
//}

//bool Driver::unsubscribeFromNotification(const QString& name)
//{
//    return unsubscribeFromNotificationImplementation(name);
//}

//QStringList Driver::subscribedToNotifications() const
//{
//    return subscribedToNotificationsImplementation();
//}
//#endif

//bool Driver::subscribeToNotificationImplementation(const QString& name)
//{
//}

//bool Driver::unsubscribeFromNotificationImplementation(const QString& name)
//{
//}

//QStringList Driver::subscribedToNotificationsImplementation() const
//{
//}

QString Driver::escapeIdentifier(const QString& identifier, IdentifierType) const
{
    // Отладить
    break_point

    QString res = identifier;
    if(!identifier.isEmpty()
       && !identifier.startsWith(QLatin1Char('"'))
       && !identifier.endsWith(QLatin1Char('"')) )
    {
        res.replace(QLatin1Char('"'), QLatin1String("\"\""));
        res.prepend(QLatin1Char('"')).append(QLatin1Char('"'));
        res.replace(QLatin1Char('.'), QLatin1String("\".\""));
    }
    return res;
}

void Driver::abortOperation()
{
    log_verbose_m << "Abort sql-operation"
                  << ". Connect: " << addrToNumber(_connect)
                  << " (call from thread: " << trd::gettid() << ")";

    _operationIsAborted = true;

    if (PGcancel* cancel = PQgetCancel(_connect))
    {
        const int errBuffSize = 256;
        char errBuff[errBuffSize] = {0};
        if (1 != PQcancel(cancel, errBuff, errBuffSize - 1))
        {
            const char* msg = "Failed abort sql-operation";
            setLastError(QSqlError("PostgresDriver", msg, QSqlError::UnknownError, "1"));

            log_error_m << msg << "; Detail: " << errBuff;
        }
        PQfreeCancel(cancel);
    }
}

bool Driver::operationIsAborted() const
{
    return _operationIsAborted;
}

//-------------------------------- Functions ---------------------------------

Transaction::Ptr createTransact(const DriverPtr& drv)
{
    return Transaction::Ptr(new Transaction(drv));
}

QSqlResult* createResult(const DriverPtr& driver)
{
    return new Result(driver, Result::ForwardOnly::Yes);
}

QSqlResult* createResult(const Transaction::Ptr& transact)
{
    return new Result(transact, Result::ForwardOnly::Yes);
}

} // namespace postgres
} // namespace db

#undef log_error_m
#undef log_warn_m
#undef log_info_m
#undef log_verbose_m
#undef log_debug_m
#undef log_debug2_m
