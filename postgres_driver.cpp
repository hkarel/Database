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
#include "shared/qt/quuidex.h"
#include "shared/qt/logger/logger_operators.h"
#include "shared/thread/thread_info.h"

#include <QDateTime>
#include <QRegExp>
#include <QVariant>
#include <QSqlField>
#include <QSqlIndex>
#include <QSqlQuery>
#include <QVarLengthArray>
#include <utility>
#include <stdlib.h>
#include <byteswap.h>

#define log_error_m   alog::logger().error  (__FILE__, __func__, __LINE__, "PostgresDrv")
#define log_warn_m    alog::logger().warn   (__FILE__, __func__, __LINE__, "PostgresDrv")
#define log_info_m    alog::logger().info   (__FILE__, __func__, __LINE__, "PostgresDrv")
#define log_verbose_m alog::logger().verbose(__FILE__, __func__, __LINE__, "PostgresDrv")
#define log_debug_m   alog::logger().debug  (__FILE__, __func__, __LINE__, "PostgresDrv")
#define log_debug2_m  alog::logger().debug2 (__FILE__, __func__, __LINE__, "PostgresDrv")

#define PG_TYPE_BOOL       16   // QBOOLOID
#define PG_TYPE_INT8       18   // OINT1OID
#define PG_TYPE_INT16      21   // QINT2OID
#define PG_TYPE_INT32      23   // QINT4OID
#define PG_TYPE_INT64      20   // QINT8OID

#define PG_TYPE_BYTEARRAY  17   // QBYTEARRAY (BINARY)
#define PG_TYPE_STRING     25   // STRING (NOT BIN)

#define PG_TYPE_FLOAT      700  // QFLOAT4OID
#define PG_TYPE_DOUBLE     701  // QFLOAT8OID

#define PG_TYPE_DATE       1082 // QDATEOID
#define PG_TYPE_TIME       1083 // QTIMEOID
#define PG_TYPE_TIMESTAMP  1114 // QTIMESTAMPOID

#define PG_TYPE_UUID       2950

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

/*
QVariant::Type qFirebirdTypeName(int iType, bool hasScale)
{
    switch (iType)
    {
        case blr_varying:
        case blr_varying2:
        case blr_text:
        case blr_cstring:
        case blr_cstring2:
            //break_point
            return QVariant::String;

        case blr_sql_time:
            return QVariant::Time;

        case blr_sql_date:
            return QVariant::Date;

        case blr_timestamp:
            return QVariant::DateTime;

        case blr_blob:
            break_point
            return QVariant::ByteArray;

        case blr_quad:
        case blr_short:
        case blr_long:
            return (hasScale) ? QVariant::Double : QVariant::Int;

        case blr_int64:
            return (hasScale) ? QVariant::Double : QVariant::LongLong;

        case blr_float:
        case blr_d_float:
        case blr_double:
            return QVariant::Double;
    }
    log_warn_m << "qFirebirdTypeName(): unknown datatype: " << iType;
    return QVariant::Invalid;
}
*/

/*
QVariant::Type qFirebirdTypeName2(int iType, bool hasScale, int subType, int subLength)
{
    switch (iType & ~1)
    {
        case SQL_VARYING:
        case SQL_TEXT:
            // [Karelin]
            // return QVariant::String;
            // return (subType == 1 OCTET) ? QVariant::ByteArray : QVariant::String;
            if (subType == 1 OCTET)
            {
                if (subLength == 16)
                {
                    //int varType1 = QMetaTypeId<Uuid>::qt_metatype_id();
                    //int varType2 = qMetaTypeId<Uuid>();
                    return QVariant::Type(qMetaTypeId<QUuidEx>());
                }
                else
                    return QVariant::ByteArray;
            }
            else
                return QVariant::String;

        case SQL_LONG:
        case SQL_SHORT:
            return (hasScale) ? QVariant::Double : QVariant::Int;

        case SQL_INT64:
            return (hasScale) ? QVariant::Double : QVariant::LongLong;

        case SQL_FLOAT:
        case SQL_DOUBLE:
            return QVariant::Double;

        case SQL_TIMESTAMP:
            return QVariant::DateTime;

        case SQL_TYPE_TIME:
            return QVariant::Time;

        case SQL_TYPE_DATE:
            return QVariant::Date;

        case SQL_ARRAY:
            return QVariant::List;

        case SQL_BLOB:
            return QVariant::ByteArray;

        default:
            return QVariant::Invalid;
    }
}
*/


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

        default:
            return QVariant::Invalid;
    }
}

qint64 toTimeStamp(const QDateTime& dt)
{
//    static const QTime midnight {0, 0, 0, 0};
//    //static const QDate basedate {1858, 11, 17};
//    static const QDate basedate {2000, 01, 01};

//    ISC_TIMESTAMP ts;
//    ts.timestamp_time = midnight.msecsTo(dt.time()) * 1000;
//    ts.timestamp_date = basedate.daysTo(dt.date());

    //break_point
    // отладить

    return dt.toMSecsSinceEpoch() * 1000;
}

QDateTime fromTimeStamp(qint64 ts)
{
    static const QDateTime basedate {QDate{2000, 01, 01}};
    return basedate.addMSecs(ts / 1000);
}

qint64 toTime(const QTime& t)
{
    qint64 fromMidnight = (qint64)t.msecsSinceStartOfDay() * 1000;
    return fromMidnight;
}

QTime fromTime(qint64 pgtime)
{
    static const QTime midnight {0, 0, 0, 0};
    QTime t = midnight.addMSecs(int(pgtime / 1000));
    return t;
}

qint32 toDate(const QDate& d)
{
    //static const QDate basedate {1858, 11, 17};
    static const QDate basedate {2000, 01, 01};
    return basedate.daysTo(d);
}

QDate fromDate(qint32 pgdate)
{
    //static const QDate basedate {1858, 11, 17};
    static const QDate basedate {2000, 01, 01};
    QDate d = basedate.addDays(pgdate);
    return d;
}

/*
QByteArray encodeString(const QTextCodec* tc, const QString& str)
{
    if (tc)
        return tc->fromUnicode(str);
    return str.toUtf8();
}

template<typename T>
QList<QVariant> toList(char** buf, int count, T* = 0)
{
    QList<QVariant> res;
    for (int i = 0; i < count; ++i)
    {
        res.append(*(T*)(*buf));
        *buf += sizeof(T);
    }
    return res;
}

// char** ? seems like bad influence from oracle ...
template<>
QList<QVariant> toList<long>(char** buf, int count, long*)
{
    QList<QVariant> res;
    for (int i = 0; i < count; ++i)
    {
        if (sizeof(int) == sizeof(long))
            res.append(int((*(long*)(*buf))));
        else
            res.append((qint64)(*(long*)(*buf)));

        *buf += sizeof(long);
    }
    return res;
}

char* readArrayBuffer(QList<QVariant>& list, char* buffer, short curDim,
                      short* numElements, ISC_ARRAY_DESC* arrayDesc,
                      const QTextCodec* tc)
{
    const short dim = arrayDesc->array_desc_dimensions - 1;
    const unsigned char dataType = arrayDesc->array_desc_dtype;
    QList<QVariant> valList;
    unsigned short strLen = arrayDesc->array_desc_length;

    if (curDim != dim)
    {
        for (int i = 0; i < numElements[curDim]; ++i)
            buffer = readArrayBuffer(list, buffer, curDim + 1,
                                     numElements, arrayDesc, tc);
    }
    else
    {
        switch (dataType)
        {
            case blr_varying:
            case blr_varying2:
                break_point
                strLen += 2; // for the two terminating null values
                // FALLTHRU - reserved words for fix GCC 7 warning

            case blr_text:
            case blr_text2:
                for (int i = 0; i < numElements[dim]; ++i)
                {
                    int o;
                    for (o = 0; o < strLen && buffer[o] != 0; ++o) {}
                    if (tc)
                        valList.append(tc->toUnicode(buffer, o));
                    else
                        valList.append(QString::fromUtf8(buffer, o));

                    buffer += strLen;
                }
                break;

            case blr_long:
                valList = toList<long>(&buffer, numElements[dim], static_cast<long*>(0));
                break;

            case blr_short:
                valList = toList<short>(&buffer, numElements[dim]);
                break;

            case blr_int64:
                valList = toList<qint64>(&buffer, numElements[dim]);
                break;

            case blr_float:
                valList = toList<float>(&buffer, numElements[dim]);
                break;

            case blr_double:
                valList = toList<double>(&buffer, numElements[dim]);
                break;

            case blr_timestamp:
                for(int i = 0; i < numElements[dim]; ++i)
                {
                    valList.append(fromTimeStamp(buffer));
                    buffer += sizeof(ISC_TIMESTAMP);
                }
                break;

            case blr_sql_time:
                for(int i = 0; i < numElements[dim]; ++i)
                {
                    valList.append(fromTime(buffer));
                    buffer += sizeof(ISC_TIME);
                }
                break;

            case blr_sql_date:
                for(int i = 0; i < numElements[dim]; ++i)
                {
                    valList.append(fromDate(buffer));
                    buffer += sizeof(ISC_DATE);
                }
                break;
        }
    }
    if (dim > 0)
    {
        break_point
        list.append(valList);
    }
    else
    {
        break_point
        list += valList;
    }
    return buffer;
}

template<typename T>
char* fillList(char* buffer, const QList<QVariant>& list, T* = 0)
{
    for (int i = 0; i < list.size(); ++i)
    {
        T val;
        val = qvariant_cast<T>(list.at(i));
        memcpy(buffer,& val, sizeof(T));
        buffer += sizeof(T);
    }
    return buffer;
}

template<>
char* fillList<float>(char* buffer, const QList<QVariant>& list, float*)
{
    for (int i = 0; i < list.size(); ++i)
    {
        double val;
        float val2 = 0;
        val = qvariant_cast<double>(list.at(i));
        val2 = (float)val;
        memcpy(buffer,& val2, sizeof(float));
        buffer += sizeof(float);
    }
    return buffer;
}

char* qFillBufferWithString(char* buffer, short buflen, const QString& string,
                            bool varying, bool array, const QTextCodec* tc)
{
    // keep a copy of the string alive in this scope
    QByteArray ba = encodeString(tc, string);
    if (varying)
    {
        short tmpBuflen = buflen;
        if (ba.length() < buflen)
            buflen = ba.length();

        if (array) // interbase stores varying arrayelements different than normal varying elements
        {
            // [Karelin]
            // memcpy(buffer, str.data(), buflen);
            memcpy(buffer, (char*)ba.constData(), buflen);
            memset(buffer + buflen, 0, tmpBuflen - buflen);
        }
        else
        {
            *(short*)buffer = buflen; // first two bytes is the length
            // [Karelin]
            // memcpy(buffer + sizeof(short), str.data(), buflen);
            memcpy(buffer + sizeof(short), (char*)ba.constData(), buflen);

        }
        buffer += tmpBuflen;
    }
    else
    {
        ba = ba.leftJustified(buflen, ' ', true);
        // [Karelin]
        // memcpy(buffer, str.data(), buflen);
        memcpy(buffer, (char*)ba.constData(), buflen);
        buffer += buflen;
    }
    return buffer;
}

// [Karelin]
char* qFillBufferWithByteArray(char* buffer, short buflen, const QByteArray& ba,
                               bool varying)
{
    if (varying)
    {
        short tmpBuflen = buflen;
        if (ba.length() < buflen)
            buflen = ba.length();

        //if (array) { // interbase stores varying arrayelements different than normal varying elements
        //    memcpy(buffer, str.data(), buflen);
        //    memset(buffer + buflen, 0, tmpBuflen - buflen);
        //} else {
        *(short*)buffer = buflen; // first two bytes is the length
        memcpy(buffer + sizeof(short), (char*)ba.constData(), buflen);
        //}
        buffer += tmpBuflen;
    }
    else
    {
        const QByteArray& str2 = ba.leftJustified(buflen, 0, true);
        memcpy(buffer, (char*)str2.constData(), buflen);
        buffer += buflen;
    }
    return buffer;
}

char* createArrayBuffer(char* buffer, const QList<QVariant>& list,
                        QVariant::Type type, short curDim, ISC_ARRAY_DESC* arrayDesc,
                        QString& error, const QTextCodec* tc)
{
    int i;
    ISC_ARRAY_BOUND* bounds = arrayDesc->array_desc_bounds;
    short dim = arrayDesc->array_desc_dimensions - 1;

    int elements = (bounds[curDim].array_bound_upper -
                    bounds[curDim].array_bound_lower + 1);

    if (list.size() != elements) // size mismatch
    {
        break_point
        error = QLatin1String("Array size mismatch. Fieldname: %1. ")
                + QString("Expected size: %1, supplied size: %2").arg(elements)
                                                                 .arg(list.size());
        return 0;
    }

    if (curDim != dim)
    {
        for (i = 0; i < list.size(); ++i)
        {
            if (list.at(i).type() != QVariant::List) // dimensions mismatch
            {
                error = QLatin1String("Array dimensons mismatch. Fieldname: %1");
                return 0;
            }
            buffer = createArrayBuffer(buffer, list.at(i).toList(), type,
                                       curDim + 1, arrayDesc, error, tc);
            if (!buffer)
                return 0;
        }
    }
    else
    {
        switch (type)
        {
            case QVariant::Int:
            case QVariant::UInt:
                if (arrayDesc->array_desc_dtype == blr_short)
                    buffer = fillList<short>(buffer, list);
                else
                    buffer = fillList<int>(buffer, list);
                break;

            case QVariant::Double:
                if (arrayDesc->array_desc_dtype == blr_float)
                    buffer = fillList<float>(buffer, list, static_cast<float*>(0));
                else
                    buffer = fillList<double>(buffer, list);
                break;

            case QVariant::LongLong:
                buffer = fillList<qint64>(buffer, list);
                break;

            case QVariant::ULongLong:
                buffer = fillList<quint64>(buffer, list);
                break;

            case QVariant::String:
                for (i = 0; i < list.size(); ++i)
                    buffer = qFillBufferWithString(buffer, arrayDesc->array_desc_length,
                                                   list.at(i).toString(),
                                                   arrayDesc->array_desc_dtype == blr_varying,
                                                   true, tc);
                break;

            case QVariant::Date:
                for (i = 0; i < list.size(); ++i)
                {
                   *((ISC_DATE*)buffer) = toDate(list.at(i).toDate());
                    buffer += sizeof(ISC_DATE);
                }
                break;

            case QVariant::Time:
                for (i = 0; i < list.size(); ++i)
                {
                    *((ISC_TIME*)buffer) = toTime(list.at(i).toTime());
                    buffer += sizeof(ISC_TIME);
                }
                break;

            case QVariant::DateTime:
                for (i = 0; i < list.size(); ++i)
                {
                    *((ISC_TIMESTAMP*)buffer) = toTimeStamp(list.at(i).toDateTime());
                    buffer += sizeof(ISC_TIMESTAMP);
                }
                break;

            default:
                break;
        }
    }
    return buffer;
}

//typedef QMap<void*, Driver*> QFirebirdBufferDriverMap;
//Q_GLOBAL_STATIC(QFirebirdBufferDriverMap, qBufferDriverMap)
//Q_GLOBAL_STATIC(QMutex, qMutex);

//void qFreeEventBuffer(QFirebirdEventBuffer* eBuffer)
//{
//    qMutex()->lock();
//    qBufferDriverMap()->remove(reinterpret_cast<void*>(eBuffer->resultBuffer));
//    qMutex()->unlock();
//    delete eBuffer;
//}
*/

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

    //template<> inline constexpr zview
    //begin_cmd<read_committed, write_policy::read_only>{
    //    "BEGIN READ ONLY"};
    if (isolationLevel == IsolationLevel::ReadCommitted
        && writePolicy == WritePolicy::ReadOnly)
    {
        beginCmd = "BEGIN READ ONLY";
    }
    //template<> inline constexpr zview
    //begin_cmd<repeatable_read, write_policy::read_write>{
    //    "BEGIN ISOLATION LEVEL REPEATABLE READ"};
    else if (isolationLevel == IsolationLevel::RepeatableRead
             && writePolicy == WritePolicy::ReadWrite)
    {
        beginCmd = "BEGIN ISOLATION LEVEL REPEATABLE READ";
    }
    //template<> inline constexpr zview
    //begin_cmd<repeatable_read, write_policy::read_only>{
    //    "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY"};
    else if (isolationLevel == IsolationLevel::RepeatableRead
             && writePolicy == WritePolicy::ReadOnly)
    {
        beginCmd = "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY";
    }
    //template<> inline constexpr zview
    //begin_cmd<serializable, write_policy::read_write>{
    //    "BEGIN ISOLATION LEVEL SERIALIZABLE"};
    else if (isolationLevel == IsolationLevel::Serializable
             && writePolicy == WritePolicy::ReadWrite)
    {
        beginCmd = "BEGIN ISOLATION LEVEL SERIALIZABLE";
    }
    //template<> inline constexpr zview
    //begin_cmd<serializable, write_policy::read_only>{
    //    "BEGIN ISOLATION LEVEL SERIALIZABLE READ ONLY"};
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
    _transactId = atoi(val);
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
    setLastError(QSqlError("PostgresResult", MSG, ERR_TYPE, 1)); \
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

/*
    if (_queryType == isc_info_sql_stmt_select)
    {
        return true;
    }
    else if (_queryType == isc_info_sql_stmt_exec_procedure)
    {
        if (_sqlda && (_sqlda->sqld != 0))
            return true;
    }
    return false;
*/
}

bool Result::checkError(const char* msg, QSqlError::ErrorType type,
                        const PGresult* result, const char* func, int line)
{
    int status = PQresultStatus(result);
    if (status == PGRES_FATAL_ERROR)
    {
        const char* err = PQerrorMessage(_drv->_connect);
        setLastError(QSqlError("PostgresResult", msg, type, 1));
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

/*
    ISC_STATUS status[20] = {0};
    isc_dsql_allocate_statement(status, _drv->ibase(), &_stmt);
    if (CHECK_ERROR("Could not allocate statemen", QSqlError::StatementError))
    {
        rollbackInternalTransact();
        return false;
    }

    isc_dsql_prepare(status, transact(), &_stmt, 0,
                     const_cast<char*>(encodeString(_drv->_textCodec, query).constData()),
                     FBVERSION, _sqlda);
    if (CHECK_ERROR("Could not prepare statement", QSqlError::StatementError))
    {
        rollbackInternalTransact();
        return false;
    }

    isc_dsql_describe_bind(status, &_stmt, FBVERSION, _inda);
    if (CHECK_ERROR("Could not describe input statement", QSqlError::StatementError))
    {
        rollbackInternalTransact();
        return false;
    }

    if (_inda->sqld > _inda->sqln)
    {
        if (!enlargeDA(_inda, _inda->sqld))
        {
            rollbackInternalTransact();
            return false;
        }

        isc_dsql_describe_bind(status, &_stmt, FBVERSION, _inda);
        if (CHECK_ERROR("Could not describe input statement", QSqlError::StatementError))
        {
            rollbackInternalTransact();
            return false;
        }
    }
    initDA(_inda);

    if (_sqlda->sqld > _sqlda->sqln)
    {
        // need more field descriptors
        if (!enlargeDA(_sqlda, _sqlda->sqld))
        {
            rollbackInternalTransact();
            return false;
        }

        isc_dsql_describe(status, &_stmt, FBVERSION, _sqlda);
        if (CHECK_ERROR("Could not describe statement", QSqlError::StatementError))
        {
            rollbackInternalTransact();
            return false;
        }
    }

    // Определяем тип sql-запроса
    char acBuffer[9];
    char qType = isc_info_sql_stmt_type;
    isc_dsql_sql_info(status, &_stmt, 1, &qType, sizeof(acBuffer), acBuffer);
    if (CHECK_ERROR("Could not get query info", QSqlError::StatementError))
    {
        rollbackInternalTransact();
        return false;
    }
    int iLength = isc_vax_integer(&acBuffer[1], 2);
    _queryType = isc_vax_integer(&acBuffer[3], iLength);
    //---

    if (isSelectSql())
    {
        setSelect(true);
        initDA(_sqlda);
    }
    else
    {
        setSelect(false);
        deleteDA(_sqlda);
    }
*/

    _preparedQuery = pgQuery;

    log_debug2_m << "End prepare query"
                 << ". Transact: " << addrToNumber(_drv->_connect) << "/" << transactId();
    return true;
}

struct QueryParams
{
    int    nparams      = {0};
    char** paramValues  = {0};
    int*   paramLengths = {0};
    int*   paramFormats = {0};

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
    ~QueryParams()
    {
        for (int i = 0; i < nparams; ++i)
            free(paramValues[i]);

        delete [] paramValues;
        delete [] paramLengths;
        delete [] paramFormats;
    }
};

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

            if (val.isNull())
            {
                continue;
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
                        QString msg = "Param %1 is not UUID type. Transact: %2/%3";
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
                default:
                {
                    QString msg = "Param %1, is unknown datatype: %2. Transact: %2/%3";
                    msg = msg.arg(i)
                             .arg(paramtype)
                             .arg(addrToNumber(_drv->_connect))
                             .arg(transactId());
                    SET_LAST_ERROR(msg, QSqlError::StatementError)
                    rollbackInternalTransact();
                    return false;
                }
            }
        }
    }

/*
    if (_inda)
    {
        const QVector<QVariant>& values = boundValues();
        if (alog::logger().level() == alog::Level::Debug2)
        {
            for (int i = 0; i < values.count(); ++i)
                log_debug2_m << "Query param" << i << ": " << values[i];
        }
        if (values.count() != _inda->sqld)
        {
            QString msg = "Parameter mismatch, expected %1, got %2 parameters"
                          ". Transact: %3/%4";
            msg = msg.arg(_inda->sqld)
                     .arg(values.count())
                     .arg(_drv->_ibase)
                     .arg(*transact());
            SET_LAST_ERROR(msg, QSqlError::StatementError)
            rollbackInternalTransact();
            return false;
        }
        for (int i = 0; i < values.count(); ++i)
        {
            const QVariant& val = values[i];
            if (!val.isValid())
            {
                QString msg = "Query param%1 is invalid. Transact: %2/%3";
                msg = msg.arg(i)
                         .arg(_drv->_ibase)
                         .arg(*transact());
                SET_LAST_ERROR(msg, QSqlError::StatementError)
                rollbackInternalTransact();
                return false;
            }

            XSQLVAR& sqlVar = _inda->sqlvar[i];
            if (!sqlVar.sqldata)
            {
                // skip unknown datatypes
                log_error_m << "FireBird unknown datatype"
                            << ". Transact: " << _drv->_ibase << "/" << *transact();
                continue;
            }
            if (sqlVar.sqltype & 1)
            {
                // В эту секцию попадаем если поле может быть установлено в NULL,
                // т.е. для этого поля не выставлен признак "NOT NULL".
                // См. описание для поля XSQLVAR::sqltype
                // https://firebirder.ru/xsqlvarsqltype-i-xsqlvarsqlind
                if (val.isNull())
                {
                    // set null indicator
                    *(sqlVar.sqlind) = -1;
                    // and set the value to 0, otherwise it would count as empty string.
                    // it seems to be working with just setting sqlind to -1
                    // *((char*)d->inda->sqlvar[para].sqldata) = 0;
                    continue;
                }
                else if (val.userType() == qMetaTypeId<QUuidEx>())
                {
                    const QUuidEx& uuid = val.value<QUuidEx>();
                    if (uuid.isNull())
                    {
                        *(sqlVar.sqlind) = -1;
                        continue;
                    }
                }
                else if (val.userType() == qMetaTypeId<QUuid>())
                {
                    const QUuid& uuid = val.value<QUuid>();
                    if (uuid.isNull())
                    {
                        *(sqlVar.sqlind) = -1;
                        continue;
                    }
                }
                // a value of 0 means non-null.
                *(sqlVar.sqlind) = 0;
            }
//             // Для полей CHAR с кодировкой OCTET
//             if (iType == 452) {
//                 //return QVariant::ByteArray;
//                 ok& = d->writeBlob(para, val.toByteArray());
//                 continue;
//             }

            switch (sqlVar.sqltype & ~1)
            {
                case SQL_INT64:
                    if (sqlVar.sqlscale < 0)
                    {
                        *((qint64*)sqlVar.sqldata) =
                            (qint64)floor(0.5 + val.toDouble() * pow(10.0, sqlVar.sqlscale * -1));
                    }
                    else
                        *((qint64*)sqlVar.sqldata) = val.toLongLong();
                    break;

                case SQL_LONG:
                    if (sqlVar.sqllen == 4)
                    {
                        if (sqlVar.sqlscale < 0)
                        {
                            *((qint32*)sqlVar.sqldata) =
                                (qint32)floor(0.5 + val.toDouble() * pow(10.0, sqlVar.sqlscale * -1));
                        }
                        else
                            *((qint32*)sqlVar.sqldata) = (qint32)val.toInt();
                    }
                    else
                        *((qint64*)sqlVar.sqldata) = val.toLongLong();
                    break;

                case SQL_SHORT:
                    if (sqlVar.sqlscale < 0)
                    {
                        *((short*)sqlVar.sqldata) =
                            (short)floor(0.5 + val.toDouble() * pow(10.0, sqlVar.sqlscale * -1));
                    }
                    else
                       *((short*)sqlVar.sqldata) = (short)val.toInt();
                    break;

                case SQL_FLOAT:
                    *((float*)sqlVar.sqldata) = (float)val.toDouble();
                    break;

                case SQL_DOUBLE:
                    *((double*)sqlVar.sqldata) = val.toDouble();
                    break;

                case SQL_TIMESTAMP:
                    *((ISC_TIMESTAMP*)sqlVar.sqldata) = toTimeStamp(val.toDateTime());
                    break;

                case SQL_TYPE_TIME:
                   *((ISC_TIME*)sqlVar.sqldata) = toTime(val.toTime());
                    break;

                case SQL_TYPE_DATE:
                   *((ISC_DATE*)sqlVar.sqldata) = toDate(val.toDate());
                    break;

                case SQL_VARYING:
                case SQL_TEXT:
                {
                    bool varying = ((sqlVar.sqltype & ~1) == SQL_VARYING);

                    // [Karelin]
                    if (sqlVar.sqlsubtype == 1 OCTET)
                    {
                        QByteArray ba;
                        if (val.userType() == qMetaTypeId<QUuidEx>())
                        {
                            const QUuidEx& uuid = val.value<QUuidEx>();
                            ba = uuid.toRfc4122();
                        }
                        else if (val.userType() == qMetaTypeId<QUuid>())
                        {
                            const QUuid& uuid = val.value<QUuid>();
                            ba = uuid.toRfc4122();
                        }
                        else
                            ba = val.toByteArray();

                        qFillBufferWithByteArray(sqlVar.sqldata, sqlVar.sqllen,
                                                 ba, varying);
                    }
                    else
                        qFillBufferWithString(sqlVar.sqldata, sqlVar.sqllen,
                                              val.toString(), varying,
                                              false, _drv->_textCodec);
                    break;
                }
                case SQL_BLOB:
                    ok &= writeBlob(sqlVar, val.toByteArray());
                    break;

                case SQL_ARRAY:
                    ok &= writeArray(sqlVar, val.toList());
                    break;

                default:
                    log_error_m << "Unknown datatype: " << (sqlVar.sqltype & ~1)
                                << ". Transact: " << _drv->_ibase << "/" << *transact();
            }
        }
    }
*/

/*
    ISC_STATUS status[20] = {0};

    if (colCount()
        && _queryType != isc_info_sql_stmt_exec_procedure)
    {
        isc_dsql_free_statement(status, &_stmt, DSQL_close);
        if (CHECK_ERROR("Unable to close statement", QSqlError::UnknownError))
        {
            rollbackInternalTransact();
            return false;
        }
        cleanup();
    }

    if (_queryType == isc_info_sql_stmt_exec_procedure)
        isc_dsql_execute2(status, transact(), &_stmt, FBVERSION, _inda, _sqlda);
    else
        isc_dsql_execute(status, transact(), &_stmt, FBVERSION, _inda);

    if (_drv->operationIsAborted())
    {
        SET_LAST_ERROR("Sql-operation aborted", QSqlError::UnknownError)
        rollbackInternalTransact();
        return false;
    }

    if (CHECK_ERROR("Unable to execute query", QSqlError::UnknownError))
    {
        rollbackInternalTransact();
        return false;
    }

    // Not all stored procedures necessarily return values.
    if (_queryType == isc_info_sql_stmt_exec_procedure
        && _sqlda
        && _sqlda->sqld == 0)
    {
        deleteDA(_sqlda);
    }
    if (_sqlda)
        init(_sqlda->sqld);
*/

    if (1 != PQsendQueryPrepared(_drv->_connect, _stmtName,
                                 nparams,             // int nParams,
                                 params.paramValues,  // const char * const *paramValues,
                                 params.paramLengths, // const int *paramLengths,
                                 params.paramFormats, // const int *paramFormats,
                                 1 ))                 // int resultFormat
    {
        SET_LAST_ERROR("Failed call PQsendQueryPrepared()", QSqlError::UnknownError)
        rollbackInternalTransact();
        return false;
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
                // break_point
                // отладить

                int fsize = PQfsize(_stmt, i);
                row[idx] = QByteArray(value, fsize);
                break;
            }
            case PG_TYPE_STRING:
                row[idx] = QString::fromUtf8(value).trimmed();
                break;

            case PG_TYPE_UUID:
            {
                int fsize = PQfsize(_stmt, i);
                const QUuid& uuid =
                    QUuidEx::fromRfc4122(QByteArray::fromRawData(value, fsize));
                const QUuidEx& uuidex = static_cast<const QUuidEx&>(uuid);
                row[idx].setValue(uuidex);
                break;
            }
            default:
                row[idx] = QVariant();
        }
    }

/*
    for (int i = 0; i < _sqlda->sqld; ++i)
    {
        int idx = rowIdx + i;
        XSQLVAR& sqlVar = _sqlda->sqlvar[i];
        Q_ASSERT(sqlVar.sqldata);

        if ((sqlVar.sqltype & 1) && *sqlVar.sqlind)
        {
            // null value
            QVariant v;
            v.convert(qFirebirdTypeName2(sqlVar.sqltype, sqlVar.sqlscale < 0,
                                         sqlVar.sqlsubtype, sqlVar.sqllen));
            if (v.type() == QVariant::Double)
            {
                switch (numericalPrecisionPolicy())
                {
                    case QSql::LowPrecisionInt32:
                        v.convert(QVariant::Int);
                        break;

                    case QSql::LowPrecisionInt64:
                        v.convert(QVariant::LongLong);
                        break;

                    case QSql::HighPrecision:
                        v.convert(QVariant::String);
                        break;

                    case QSql::LowPrecisionDouble:
                        // no conversion
                        break;
                }
            }
            row[idx] = v;
            continue;
        }

        switch (sqlVar.sqltype & ~1)
        {
            case SQL_VARYING:
                // pascal strings - a short with a length information followed by the data
                if (_drv->_textCodec)
                    row[idx] = _drv->_textCodec->toUnicode(sqlVar.sqldata + sizeof(short), *(short*)sqlVar.sqldata);
                else
                    row[idx] = QString::fromUtf8(sqlVar.sqldata + sizeof(short), *(short*)sqlVar.sqldata);
                break;

            case SQL_INT64:
                if (sqlVar.sqlscale < 0)
                    row[idx] = *(qint64*)sqlVar.sqldata * pow(10.0, sqlVar.sqlscale);
                else
                    row[idx] = QVariant(*(qint64*)sqlVar.sqldata);
                break;

            case SQL_LONG:
                if (sqlVar.sqllen == 4)
                {
                    if (sqlVar.sqlscale < 0)
                        row[idx] = QVariant(*(qint32*)sqlVar.sqldata * pow(10.0, sqlVar.sqlscale));
                    else
                        row[idx] = QVariant(*(qint32*)sqlVar.sqldata);
                }
                else
                    row[idx] = QVariant(*(qint64*)sqlVar.sqldata);
                break;

            case SQL_SHORT:
                if (sqlVar.sqlscale < 0)
                    row[idx] = QVariant(long((*(short*)sqlVar.sqldata)) * pow(10.0, sqlVar.sqlscale));
                else
                    row[idx] = QVariant(int((*(short*)sqlVar.sqldata)));
                break;

            case SQL_FLOAT:
                row[idx] = QVariant(double((*(float*)sqlVar.sqldata)));
                break;

            case SQL_DOUBLE:
                row[idx] = QVariant(*(double*)sqlVar.sqldata);
                break;

            case SQL_TIMESTAMP:
                row[idx] = fromTimeStamp(sqlVar.sqldata);
                break;

            case SQL_TYPE_TIME:
                row[idx] = fromTime(sqlVar.sqldata);
                break;

            case SQL_TYPE_DATE:
                row[idx] = fromDate(sqlVar.sqldata);
                break;

            case SQL_TEXT:
                // [Karelin]
                if (sqlVar.sqlsubtype == 1 OCTET)
                {
                    if (sqlVar.sqllen == 16)
                    {
                        const QUuid& uuid = QUuidEx::fromRfc4122(QByteArray::fromRawData(
                                                                 sqlVar.sqldata, sqlVar.sqllen));
                        const QUuidEx& uuidex = static_cast<const QUuidEx&>(uuid);
                        row[idx].setValue(uuidex);
                    }
                    else
                        row[idx] = QByteArray(sqlVar.sqldata, sqlVar.sqllen);
                }
                else
                {
                    if (_drv->_textCodec)
                        // [Karelin]
                        // row[idx] = d->tc->toUnicode(buf, size);
                        row[idx] = _drv->_textCodec->toUnicode(sqlVar.sqldata, sqlVar.sqllen).trimmed();
                    else
                        // [Karelin]
                        // row[idx] = QString::fromUtf8(buf, size);
                        row[idx] = QString::fromUtf8(sqlVar.sqldata, sqlVar.sqllen).trimmed();
                }
                break;

            case SQL_BLOB:
                row[idx] = fetchBlob((ISC_QUAD*)sqlVar.sqldata);
                break;

            case SQL_ARRAY:
                row[idx] = fetchArray(sqlVar, (ISC_QUAD*)sqlVar.sqldata);
                break;

            default:
                // unknown type - don't even try to fetch
                row[idx] = QVariant();
        }
        if (sqlVar.sqlscale < 0)
        {
            QVariant v = row[idx];
            switch (numericalPrecisionPolicy())
            {
                case QSql::LowPrecisionInt32:
                    if (v.convert(QVariant::Int))
                        row[idx] = v;
                    break;

                case QSql::LowPrecisionInt64:
                    if (v.convert(QVariant::LongLong))
                        row[idx] = v;
                    break;

                case QSql::LowPrecisionDouble:
                    if (v.convert(QVariant::Double))
                        row[idx] = v;
                    break;

                case QSql::HighPrecision:
                    if (v.convert(QVariant::String))
                        row[idx] = v;
                    break;
            }
        }
    }
*/

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

/*
    char cCountType;
    char acCountInfo[] = {isc_info_sql_records};

    switch (_queryType)
    {
        case isc_info_sql_stmt_select:
            cCountType = isc_info_req_select_count;
            break;

        case isc_info_sql_stmt_update:
            cCountType = isc_info_req_update_count;
            break;

        case isc_info_sql_stmt_delete:
            cCountType = isc_info_req_delete_count;
            break;

        case isc_info_sql_stmt_insert:
            cCountType = isc_info_req_insert_count;
            break;

        default:
            log_warn_m << "numRowsAffected(): Unknown statement type (" << _queryType << ")";
            return -1;
    }

    char acBuffer[33];
    int iResult = -1;
    ISC_STATUS status[20] = {0};
    isc_dsql_sql_info(status, &_stmt, sizeof(acCountInfo), acCountInfo, sizeof(acBuffer), acBuffer);
    if (CHECK_ERROR("Could not get statement info", QSqlError::StatementError))
        return -1;

    for (char* pcBuf = acBuffer + 3; *pcBuf != isc_info_end; nothing)
    {
        char cType = *pcBuf++;
        short sLength = isc_vax_integer(pcBuf, 2);
        pcBuf += 2;
        int iValue = isc_vax_integer(pcBuf, sLength);
        pcBuf += sLength;
        if (cType == cCountType)
        {
            iResult = iValue;
            break;
        }
    }
    return iResult;
*/
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

/*
        if (d->drv_d_func()->isUtf8)
            f.setName(QString::fromUtf8(PQfname(d->result, i)));
        else
            f.setName(QString::fromLocal8Bit(PQfname(d->result, i)));
        int ptype = PQftype(d->result, i);
        f.setType(qDecodePSQLType(ptype));
        int len = PQfsize(d->result, i);
        int precision = PQfmod(d->result, i);

        switch (ptype) {
        case QTIMESTAMPOID:
        case QTIMESTAMPTZOID:
            precision = 3;
            break;

        case QNUMERICOID:
            if (precision != -1) {
                len = (precision >> 16);
                precision = ((precision - VARHDRSZ) & 0xffff);
            }
            break;
        case QBITOID:
        case QVARBITOID:
            len = precision;
            precision = -1;
            break;
        default:
            if (len == -1 && precision >= VARHDRSZ) {
                len = precision - VARHDRSZ;
                precision = -1;
            }
        }

        f.setLength(len);
        f.setPrecision(precision);
        f.setSqlType(ptype);
        info.append(f);
*/
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
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, 1));

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

    if (!connOpts.isEmpty())
    {
        QString opt = connOpts;
        opt.replace(QChar(';'), QChar(' '), Qt::CaseInsensitive);
        connString.append(QChar(' ')).append(opt);
    }

    log_verbose_m << "Try open database '" << db << "'"
                  << ". User: " << user
                  << ", host: " << host
                  << ", port: " << port;

    _connect = PQconnectdb(connString.toUtf8());
    if (PQstatus(_connect) == CONNECTION_BAD)
    {
        const char* msg = "Error opening database";
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, 1));

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
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, 1));

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
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, 1));

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
            setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, 1));

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
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, 1));

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
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, 1));

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
        setLastError(QSqlError("PostgresDriver", msg, QSqlError::ConnectionError, 1));

        log_error_m << msg;

        setOpenError(true);
        PQfinish(_connect);
        _connect = 0;
        return false;
    }

    _threadId = trd::gettid();

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
    //return _isOpen;
    return (PQstatus(_connect) == CONNECTION_OK);
}

void Driver::setOpen(bool val)
{
    QSqlDriver::setOpen(val);
    //_isOpen = val;
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

//QVariant Driver::handle() const
//{
//    return QVariant(qRegisterMetaType<isc_db_handle>("isc_db_handle"), &_ibase);
//}

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
    //break_point
    log_debug2_m << "Call beginTransaction()";
    return false;
}

bool Driver::commitTransaction()
{
    //break_point
    log_debug2_m << "Call commitTransaction()";
    return false;
}

bool Driver::rollbackTransaction()
{
    //break_point
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

//bool Driver::checkError(const char* msg, QSqlError::ErrorType type,
//                        const char* func, int line)
//{
////    ISC_LONG sqlcode; QString err;
////    if (firebirdError(status, _textCodec, sqlcode, err))
////    {
////        setLastError(QSqlError("PostgresDriver", msg, type, 1));
////        alog::logger().error(__FILE__, func, line, "PostgresDrv")
////            << msg << "; Detail: " << err << "; SqlCode: " << sqlcode;
////        return true;
////    }
//    return false;
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
    // Отладить
    break_point

    _operationIsAborted = true;

    if (PGcancel* cancel = PQgetCancel(_connect))
    {
        const int errBuffSize = 256;
        char errBuff[errBuffSize] = {0};
        if (1 != PQcancel(cancel, errBuff, errBuffSize - 1))
        {
            const char* msg = "Failed abort sql-operation";
            setLastError(QSqlError("PostgresDriver", msg, QSqlError::UnknownError, 1));

            log_error_m << msg << "; Detail: " << errBuff;
        }
        PQfreeCancel(cancel);
    }

//    ISC_STATUS status[20] = {0};
//    //fb_cancel_operation(status, &_ibase, fb_cancel_raise);
//    fb_cancel_operation(status, &_ibase, fb_cancel_abort);
//    CHECK_ERROR("Failed abort sql-operation", QSqlError::UnknownError);
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
