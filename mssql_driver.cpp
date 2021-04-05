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

#include "mssql_driver.h"

#include "shared/break_point.h"
#include "shared/prog_abort.h"
#include "shared/safe_singleton.h"
#include "shared/logger/logger.h"
#include "shared/logger/format.h"
#include "shared/qt/logger_operators.h"
#include "shared/qt/quuidex.h"
#include "shared/thread/thread_utils.h"

#include "qmetatypes.h"
#include <byteswap.h>

#define log_error_m   alog::logger().error   (alog_line_location, "MssqlDrv")
#define log_warn_m    alog::logger().warn    (alog_line_location, "MssqlDrv")
#define log_info_m    alog::logger().info    (alog_line_location, "MssqlDrv")
#define log_verbose_m alog::logger().verbose (alog_line_location, "MssqlDrv")
#define log_debug_m   alog::logger().debug   (alog_line_location, "MssqlDrv")
#define log_debug2_m  alog::logger().debug2  (alog_line_location, "MssqlDrv")




namespace db {
namespace mssql {


inline quint64 addrToNumber(void* addr)
{
    return reinterpret_cast<QIntegerForSizeof<void*>::Unsigned>(addr);
}

namespace detail {

const int COLNAMESIZE = 256;
const SQLSMALLINT TABLENAMESIZE = 128;
const SQLSMALLINT qParamType[4] = { SQL_PARAM_INPUT, SQL_PARAM_INPUT, SQL_PARAM_OUTPUT, SQL_PARAM_INPUT_OUTPUT };

QString fromSQLTCHAR(const QVarLengthArray<SQLTCHAR>& input, int size=-1)
{
   QString result;

   // Remove any trailing \0 as some drivers misguidedly append one
   int realsize = qMin(size, input.size());
   if(realsize > 0 && input[realsize-1] == 0)
       realsize--;

   switch (sizeof(SQLTCHAR))
   {
       case 1:
           result=QString::fromUtf8((const char *)input.constData(), realsize);
           break;
       case 2:
           result=QString::fromUtf16((const ushort *)input.constData(), realsize);
           break;
       case 4:
           result=QString::fromUcs4((const uint *)input.constData(), realsize);
           break;
       default:
           log_error_m << "sizeof(SQLTCHAR) is %d. Don't know how to handle this.", int(sizeof(SQLTCHAR));
   }

   return result;
}

QVarLengthArray<SQLTCHAR> toSQLTCHAR(const QString &input)
{
    QVarLengthArray<SQLTCHAR> result;
    result.resize(input.size());

    switch (sizeof(SQLTCHAR))
    {
        case 1:
            memcpy(result.data(), input.toUtf8().data(), input.size());
            break;
        case 2:
            memcpy(result.data(), input.unicode(), input.size() * 2);
            break;
        case 4:
            memcpy(result.data(), input.toUcs4().data(), input.size() * 4);
            break;
        default:
            log_error_m << "sizeof(SQLTCHAR) is %d. Don't know how to handle this.", int(sizeof(SQLTCHAR));
    }
    result.append(0); // make sure it's null terminated, doesn't matter if it already is, it does if it isn't.

    return result;
}

QString qWarnODBCHandle(int handleType, SQLHANDLE handle, int *nativeCode = 0)
{
    SQLINTEGER nativeCode_ = 0;
    SQLSMALLINT msgLen = 0;
    SQLRETURN r = SQL_NO_DATA;
    SQLTCHAR state_[SQL_SQLSTATE_SIZE+1];
    QVarLengthArray<SQLTCHAR> description_(SQL_MAX_MESSAGE_LENGTH);
    QString result;
    int i = 1;

    description_[0] = 0;
    do
    {
        r = SQLGetDiagRec(handleType, handle, i, state_, &nativeCode_, 0, 0, &msgLen);
        if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && msgLen > 0)
            description_.resize(msgLen+1);

        r = SQLGetDiagRec(handleType, handle, i, state_, &nativeCode_, description_.data(), description_.size(), &msgLen);
        if (r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
        {
            if (nativeCode)
                *nativeCode = nativeCode_;
            const QString tmpstore = fromSQLTCHAR(description_, msgLen);
            if(result != tmpstore)
            {
                if(!result.isEmpty())
                    result += QLatin1Char(' ');
                result += tmpstore;
            }
        }
        else if (r == SQL_ERROR || r == SQL_INVALID_HANDLE)
        {
            return result;
        }
        ++i;
    } while (r != SQL_NO_DATA);

    return result;
}

QString qODBCWarn(const SQLHANDLE hStmt, const SQLHANDLE envHandle = 0, const SQLHANDLE pDbC = 0, int *nativeCode = 0)
{
    QString result;

    if (envHandle)
        result += qWarnODBCHandle(SQL_HANDLE_ENV, envHandle, nativeCode);

    if (pDbC)
    {
        const QString dMessage = qWarnODBCHandle(SQL_HANDLE_DBC, pDbC, nativeCode);
        if (!dMessage.isEmpty())
        {
            if (!result.isEmpty())
                result += QLatin1Char(' ');
            result += dMessage;
        }
    }
    if (hStmt)
    {
        const QString hMessage = qWarnODBCHandle(SQL_HANDLE_STMT, hStmt, nativeCode);
        if (!hMessage.isEmpty())
        {
            if (!result.isEmpty())
                result += QLatin1Char(' ');
            result += hMessage;
        }
    }

    return result;
}

QString qODBCWarn(const Result* odbc, int *nativeCode)
{
    return qODBCWarn(0, odbc->_drv->hEnv, odbc->_drv->hDbc, nativeCode);
}

void qSqlWarning(const QString& message, const Result* odbc)
{
    log_warn_m << message << "\tError:" << qODBCWarn((SQLHANDLE)odbc, 0);
}

void qSqlWarning(const QString &message, const SQLHANDLE hStmt)
{
    log_warn_m << message << "\tError:" << qODBCWarn(hStmt);
}

QSqlError qMakeError(const QString& err, QSqlError::ErrorType type, const Driver* p)
{
    int nativeCode = -1;
    QString message = qODBCWarn((SQLHANDLE)p, &nativeCode);

    return QSqlError(QLatin1String("QODBC3: ") + err, message, type, nativeCode != -1 ? QString::number(nativeCode) : QString());
}

QVariant::Type qDecodeODBCType(SQLSMALLINT sqltype, bool isSigned = true)
{
    QVariant::Type type = QVariant::Invalid;

    switch (sqltype)
    {
        case SQL_DECIMAL:
        case SQL_NUMERIC:
        case SQL_REAL:
        case SQL_FLOAT:
        case SQL_DOUBLE:
            type = QVariant::Double;
            break;
        case SQL_SMALLINT:
        case SQL_INTEGER:
        case SQL_BIT:
            type = isSigned ? QVariant::Int : QVariant::UInt;
            break;
        case SQL_TINYINT:
            type = QVariant::UInt;
            break;
        case SQL_BIGINT:
            type = isSigned ? QVariant::LongLong : QVariant::ULongLong;
            break;
        case SQL_BINARY:
        case SQL_VARBINARY:
        case SQL_LONGVARBINARY:
            type = QVariant::ByteArray;
            break;
        case SQL_DATE:
        case SQL_TYPE_DATE:
            type = QVariant::Date;
            break;
        case SQL_TIME:
        case SQL_TYPE_TIME:
            type = QVariant::Time;
            break;
        case SQL_TIMESTAMP:
        case SQL_TYPE_TIMESTAMP:
            type = QVariant::DateTime;
            break;
        case SQL_WCHAR:
        case SQL_WVARCHAR:
        case SQL_WLONGVARCHAR:
            type = QVariant::String;
            break;
        case SQL_CHAR:
        case SQL_VARCHAR:
    #if (ODBCVER >= 0x0350)
        case SQL_GUID:
            type = QVariant::Type(qMetaTypeId<QUuidEx>());
            break;
    #endif
        case SQL_LONGVARCHAR:
            type = QVariant::String;
            break;
        default:
            type = QVariant::ByteArray;
            break;
    }

    return type;
}

QString qGetStringData(SQLHANDLE hStmt, int column, int colSize, bool unicode = false)
{
    QString fieldVal;
    SQLRETURN r = SQL_ERROR;
    SQLLEN lengthIndicator = 0;

    // NB! colSize must be a multiple of 2 for unicode enabled DBs
    if (colSize <= 0)
    {
        colSize = 256;
    }
    else if (colSize > 65536)
    { // limit buffer size to 64 KB
        colSize = 65536;
    }
    else
    {
        colSize++; // make sure there is room for more than the 0 termination
    }
    if (unicode)
    {
        r = SQLGetData(hStmt, column+1, SQL_C_TCHAR, NULL, 0, &lengthIndicator);
        if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && lengthIndicator > 0)
            colSize = int(lengthIndicator / sizeof(SQLTCHAR) + 1);

        QVarLengthArray<SQLTCHAR> buf(colSize);
        memset(buf.data(), 0, colSize*sizeof(SQLTCHAR));
        while (true)
        {
            r = SQLGetData(hStmt, column+1, SQL_C_TCHAR, (SQLPOINTER)buf.data(), colSize*sizeof(SQLTCHAR), &lengthIndicator);
            if (r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
            {
                if (lengthIndicator == SQL_NULL_DATA)
                {
                    fieldVal.clear();
                    break;
                }
                // starting with ODBC Native Client 2012, SQL_NO_TOTAL is returned
                // instead of the length (which sometimes was wrong in older versions)
                // see link for more info: http://msdn.microsoft.com/en-us/library/jj219209.aspx
                // if length indicator equals SQL_NO_TOTAL, indicating that
                // more data can be fetched, but size not known, collect data
                // and fetch next block
                if (lengthIndicator == SQL_NO_TOTAL)
                {
                    fieldVal += fromSQLTCHAR(buf, colSize);
                    continue;
                }
                // if SQL_SUCCESS_WITH_INFO is returned, indicating that
                // more data can be fetched, the length indicator does NOT
                // contain the number of bytes returned - it contains the
                // total number of bytes that CAN be fetched
                int rSize = (r == SQL_SUCCESS_WITH_INFO) ? colSize : int(lengthIndicator / sizeof(SQLTCHAR));
                    fieldVal += fromSQLTCHAR(buf, rSize);
                if (lengthIndicator < SQLLEN(colSize*sizeof(SQLTCHAR)))
                {
                    // workaround for Drivermanagers that don't return SQL_NO_DATA
                    break;
                }
            }
            else if (r == SQL_NO_DATA)
            {
                break;
            }
            else
            {
                log_warn_m << "qGetStringData: Error while fetching data (" << qWarnODBCHandle(SQL_HANDLE_STMT, hStmt) << ')';
                fieldVal.clear();
                break;
            }
        }
    }
    else
    {
        r = SQLGetData(hStmt, column+1, SQL_C_CHAR, NULL, 0, &lengthIndicator);
        if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && lengthIndicator > 0)
            colSize = lengthIndicator + 1;

        QVarLengthArray<SQLCHAR> buf(colSize);
        while (true)
        {
            r = SQLGetData(hStmt, column+1, SQL_C_CHAR, (SQLPOINTER)buf.data(), colSize, &lengthIndicator);
            if (r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
            {
                if (lengthIndicator == SQL_NULL_DATA || lengthIndicator == SQL_NO_TOTAL)
                {
                    fieldVal.clear();
                    break;
                }
                // if SQL_SUCCESS_WITH_INFO is returned, indicating that
                // more data can be fetched, the length indicator does NOT
                // contain the number of bytes returned - it contains the
                // total number of bytes that CAN be fetched
                int rSize = (r == SQL_SUCCESS_WITH_INFO) ? colSize : lengthIndicator;
                // Remove any trailing \0 as some drivers misguidedly append one
                int realsize = qMin(rSize, buf.size());
                if (realsize > 0 && buf[realsize - 1] == 0)
                    realsize--;
                fieldVal += QString::fromUtf8(reinterpret_cast<const char *>(buf.constData()), realsize);
                if (lengthIndicator < SQLLEN(colSize))
                {
                    // workaround for Drivermanagers that don't return SQL_NO_DATA
                    break;
                }
            }
            else if (r == SQL_NO_DATA)
            {
                break;
            }
            else
            {
                log_warn_m << "qGetStringData: Error while fetching data (" << qWarnODBCHandle(SQL_HANDLE_STMT, hStmt) << ')';
                fieldVal.clear();
                break;
            }
        }
    }
    return fieldVal;
}

QVariant qGetBinaryData(SQLHANDLE hStmt, int column)
{
    QByteArray fieldVal;
    SQLSMALLINT colNameLen;
    SQLSMALLINT colType;
    SQLULEN colSize;
    SQLSMALLINT colScale;
    SQLSMALLINT nullable;
    SQLLEN lengthIndicator = 0;
    SQLRETURN r = SQL_ERROR;

    QVarLengthArray<SQLTCHAR> colName(COLNAMESIZE);

    r = SQLDescribeCol(hStmt, column + 1, colName.data(), COLNAMESIZE, &colNameLen, &colType, &colSize, &colScale, &nullable);
    if (r != SQL_SUCCESS)
        log_warn_m << "qGetBinaryData: Unable to describe column" << column;

    // SQLDescribeCol may return 0 if size cannot be determined
    if (!colSize)
        colSize = 255;
    else if (colSize > 65536) // read the field in 64 KB chunks
        colSize = 65536;
    fieldVal.resize(colSize);
    ulong read = 0;

    while (true)
    {
        r = SQLGetData(hStmt, column+1, SQL_C_BINARY, const_cast<char *>(fieldVal.constData() + read), colSize, &lengthIndicator);
        if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
            break;

        if (lengthIndicator == SQL_NULL_DATA)
            return QVariant(QVariant::ByteArray);
        if (lengthIndicator > SQLLEN(colSize) || lengthIndicator == SQL_NO_TOTAL)
        {
            read += colSize;
            colSize = 65536;
        }
        else
        {
            read += lengthIndicator;
        }
        if (r == SQL_SUCCESS)
        { // the whole field was read in one chunk
            fieldVal.resize(read);
            break;
        }
        fieldVal.resize(fieldVal.size() + colSize);
    }

    return fieldVal;
}

QVariant qGetGuidData(SQLHANDLE hStmt, int column)
{
    QByteArray fieldVal;
    SQLSMALLINT colNameLen;
    SQLSMALLINT colType;
    SQLULEN colSize;
    SQLSMALLINT colScale;
    SQLSMALLINT nullable;
    SQLLEN lengthIndicator = 0;
    SQLRETURN r = SQL_ERROR;

    QVarLengthArray<SQLTCHAR> colName(COLNAMESIZE);

    r = SQLDescribeCol(hStmt, column + 1, colName.data(), COLNAMESIZE, &colNameLen, &colType, &colSize, &colScale, &nullable);
    if (r != SQL_SUCCESS)
        log_warn_m << "qGetGuidData: Unable to describe column" << column;

    // SQLDescribeCol may return 0 if size cannot be determined
    if (!colSize)
        return QVariant(QVariant::Uuid);

    colSize = 16;
    fieldVal.resize(colSize);

    r = SQLGetData(hStmt, column+1, SQL_C_BINARY, const_cast<char *>(fieldVal.constData()), colSize, &lengthIndicator);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
        return QVariant(QVariant::Uuid);

    if (lengthIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::Uuid);

    QByteArray swapUuid;
    QDataStream swapStream(&swapUuid, QIODevice::WriteOnly);
    swapStream << ((quint32*) fieldVal.data())[0];
    swapStream << ((quint16*) fieldVal.data())[2];
    swapStream << ((quint16*) fieldVal.data())[3];
    swapStream << (quint64)bswap_64(((quint64*) fieldVal.data())[1]);

    return QVariant(QUuid::fromRfc4122(swapUuid));
}

QVariant qGetIntData(SQLHANDLE hStmt, int column, bool isSigned = true)
{
    SQLINTEGER intbuf = 0;
    SQLLEN lengthIndicator = 0;
    SQLRETURN r = SQLGetData(hStmt, column, isSigned ? SQL_C_SLONG : SQL_C_ULONG, (SQLPOINTER)&intbuf, sizeof(intbuf), &lengthIndicator);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
        return QVariant(QVariant::Invalid);

    if (lengthIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::Int);

    if (isSigned)
        return int(intbuf);
    else
        return uint(intbuf);
}

QVariant qGetDoubleData(SQLHANDLE hStmt, int column)
{
    SQLDOUBLE dblbuf;
    SQLLEN lengthIndicator = 0;
    SQLRETURN r = SQLGetData(hStmt, column+1, SQL_C_DOUBLE, (SQLPOINTER) &dblbuf, 0, &lengthIndicator);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        return QVariant(QVariant::Invalid);
    }
    if(lengthIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::Double);

    return (double) dblbuf;
}

QVariant qGetBigIntData(SQLHANDLE hStmt, int column, bool isSigned = true)
{
    SQLBIGINT lngbuf = 0;
    SQLLEN lengthIndicator = 0;
    SQLRETURN r = SQLGetData(hStmt, column+1, isSigned ? SQL_C_SBIGINT : SQL_C_UBIGINT, (SQLPOINTER) &lngbuf, sizeof(lngbuf), &lengthIndicator);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
        return QVariant(QVariant::Invalid);
    if (lengthIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::LongLong);

    if (isSigned)
        return qint64(lngbuf);
    else
        return quint64(lngbuf);
}

bool isAutoValue(const SQLHANDLE hStmt, int column)
{
    SQLLEN nNumericAttribute = 0; // Check for auto-increment
    const SQLRETURN r = ::SQLColAttribute(hStmt, column + 1, SQL_DESC_AUTO_UNIQUE_VALUE, 0, 0, 0, &nNumericAttribute);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        qSqlWarning(QStringLiteral("qMakeField: Unable to get autovalue attribute for column ") + QString::number(column), hStmt);
        return false;
    }
    return nNumericAttribute != SQL_FALSE;
}

// creates a QSqlField from a valid hStmt generated
// by SQLColumns. The hStmt has to point to a valid position.
QSqlField qMakeFieldInfo(const SQLHANDLE hStmt, const DriverPtr& p)
{
    QString fname = qGetStringData(hStmt, 3, -1, p->unicode);
    int type = qGetIntData(hStmt, 4).toInt(); // column type
    QSqlField f(fname, qDecodeODBCType(type, p));
    QVariant var = qGetIntData(hStmt, 6);
    f.setLength(var.isNull() ? -1 : var.toInt()); // column size
    var = qGetIntData(hStmt, 8).toInt();
    f.setPrecision(var.isNull() ? -1 : var.toInt()); // precision
    f.setSqlType(type);
    int required = qGetIntData(hStmt, 10).toInt(); // nullable-flag
    // required can be SQL_NO_NULLS, SQL_NULLABLE or SQL_NULLABLE_UNKNOWN
    if (required == SQL_NO_NULLS)
        f.setRequired(true);
    else if (required == SQL_NULLABLE)
        f.setRequired(false);
    // else we don't know
    return f;
}

QSqlField qMakeFieldInfo(const Result* p, int i )
{
    QString errorMessage;
    const QSqlField result = qMakeFieldInfo(p->hStmt, i, &errorMessage);
    if (!errorMessage.isEmpty())
        qSqlWarning(errorMessage, p);

    return result;
}

QSqlField qMakeFieldInfo(const SQLHANDLE hStmt, int i, QString *errorMessage)
{
    SQLSMALLINT colNameLen;
    SQLSMALLINT colType;
    SQLULEN colSize;
    SQLSMALLINT colScale;
    SQLSMALLINT nullable;
    SQLRETURN r = SQL_ERROR;
    QVarLengthArray<SQLTCHAR> colName(COLNAMESIZE);
    errorMessage->clear();
    r = SQLDescribeCol(hStmt, i+1, colName.data(), (SQLSMALLINT)COLNAMESIZE, &colNameLen, &colType, &colSize, &colScale, &nullable);

    if (r != SQL_SUCCESS)
    {
        *errorMessage = QStringLiteral("qMakeField: Unable to describe column ") + QString::number(i);
        return QSqlField();
    }

    SQLLEN unsignedFlag = SQL_FALSE;
    r = SQLColAttribute (hStmt, i + 1, SQL_DESC_UNSIGNED, 0, 0, 0, &unsignedFlag);
    if (r != SQL_SUCCESS)
    {
        qSqlWarning(QStringLiteral("qMakeField: Unable to get column attributes for column ")
                    + QString::number(i), hStmt);
    }

    const QString qColName(fromSQLTCHAR(colName, colNameLen));
    // nullable can be SQL_NO_NULLS, SQL_NULLABLE or SQL_NULLABLE_UNKNOWN
    QVariant::Type type = qDecodeODBCType(colType, unsignedFlag == SQL_FALSE);
    QSqlField f(qColName, type);
    f.setSqlType(colType);
    f.setLength(colSize == 0 ? -1 : int(colSize));
    f.setPrecision(colScale == 0 ? -1 : int(colScale));
    if (nullable == SQL_NO_NULLS)
        f.setRequired(true);
    else if (nullable == SQL_NULLABLE)
        f.setRequired(false);
    // else we don't know
    f.setAutoValue(isAutoValue(hStmt, i));
    QVarLengthArray<SQLTCHAR> tableName(TABLENAMESIZE);
    SQLSMALLINT tableNameLen;
    r = SQLColAttribute(hStmt, i + 1, SQL_DESC_BASE_TABLE_NAME, tableName.data(), TABLENAMESIZE, &tableNameLen, 0);
    if (r == SQL_SUCCESS)
        f.setTableName(fromSQLTCHAR(tableName, tableNameLen));

    return f;
}

} // namespace detail

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
    (void)isolationLevel;
    (void)writePolicy;

    if (!_drv->isOpen())
    {
        log_warn_m << "QODBCDriver::beginTransaction: Database not open";

        return false;
    }
    SQLUINTEGER ac(SQL_AUTOCOMMIT_OFF);
    SQLRETURN r  = SQLSetConnectAttr(_drv->hDbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)size_t(ac), sizeof(ac));
    if (r != SQL_SUCCESS)
    {
        _drv->setLastError(detail::qMakeError(QCoreApplication::translate("Unable to disable autocommit", ""), QSqlError::TransactionError, _drv));
        return false;
    }
    return true;
}

bool Transaction::commit()
{
    if (!_drv->isOpen())
    {
        log_warn_m << "QODBCDriver::commitTransaction: Database not open";
        return false;
    }
    SQLRETURN r = SQLEndTran(SQL_HANDLE_DBC, _drv->hDbc, SQL_COMMIT);
    if (r != SQL_SUCCESS)
    {
        _drv->setLastError(detail::qMakeError(QCoreApplication::translate("Unable to commit transaction", ""), QSqlError::TransactionError, _drv));
        return false;
    }
    return endTrans();
}

bool Transaction::rollback()
{
    if (!_drv->isOpen())
    {
        log_warn_m << "QODBCDriver::rollbackTransaction: Database not open";
        return false;
    }
    SQLRETURN r = SQLEndTran(SQL_HANDLE_DBC, _drv->hDbc, SQL_ROLLBACK);
    if (r != SQL_SUCCESS)
    {
        _drv->setLastError(detail::qMakeError(QCoreApplication::translate("Unable to rollback transaction", ""), QSqlError::TransactionError, _drv));
        return false;
    }
    return endTrans();
}

bool Transaction::endTrans()
{
    SQLUINTEGER ac(SQL_AUTOCOMMIT_ON);
    SQLRETURN r  = SQLSetConnectAttr(_drv->hDbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)size_t(ac), sizeof(ac));
    if (r != SQL_SUCCESS)
    {
        _drv->setLastError(detail::qMakeError(QCoreApplication::translate("Unable to enable autocommit", ""), QSqlError::TransactionError, _drv));
        return false;
    }
    return true;
}

bool Transaction::isActive() const
{
    return _isActive;
}

//---------------------------------- Result ----------------------------------


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

void Result::cleanup()
{
    log_debug2_m << "Begin dataset cleanup. Connect: " << addrToNumber(hStmt);

    if (!_externalTransact)
        if (_internalTransact && _internalTransact->isActive())
        {
             if (isSelectSql())
                 rollbackInternalTransact();
             else
                 commitInternalTransact();
        }

    SQLFreeStmt(hStmt, SQL_RESET_PARAMS);
    hStmt = nullptr;

    SqlCachedResult::cleanup();

    log_debug2_m << "End dataset cleanup. Connect: " << addrToNumber(hStmt);
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
        _drv->setLastError(detail::qMakeError(QCoreApplication::translate("Failed begin internal transaction", ""), QSqlError::TransactionError, _drv));

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
        _drv->setLastError(detail::qMakeError(QCoreApplication::translate("Failed commit internal transaction", ""), QSqlError::TransactionError, _drv));
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
        _drv->setLastError(detail::qMakeError(QCoreApplication::translate("Failed rollback internal transaction", ""), QSqlError::TransactionError, _drv));
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

bool isStmtHandleValid()
{
    return true;
}

bool Result::prepare(const QString& query)
{
    setActive(false);
    setAt(QSql::BeforeFirstRow);
    SQLRETURN r;

    recInfo.clear();
    if (hStmt && isStmtHandleValid())
    {
        r = SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
        if (r != SQL_SUCCESS)
        {
            detail::qSqlWarning(QLatin1String("QODBCResult::prepare: Unable to close statement"), hStmt);
            return false;
        }
    }
    r  = SQLAllocHandle(SQL_HANDLE_STMT, _drv->hDbc, &hStmt);
    if (r != SQL_SUCCESS)
    {
        detail::qSqlWarning(QLatin1String("QODBCResult::prepare: Unable to allocate statement handle"), hStmt);
        return false;
    }

    updateStmtHandleState();

    if (isForwardOnly())
    {
        r = SQLSetStmtAttr(hStmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, SQL_IS_UINTEGER);
    }
    else
    {
        r = SQLSetStmtAttr(hStmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_STATIC, SQL_IS_UINTEGER);
    }
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        _drv->setLastError(detail::qMakeError(QCoreApplication::translate("QODBCResult",
            "QODBCResult::reset: Unable to set 'SQL_CURSOR_STATIC' as statement attribute. "
            "Please check your ODBC driver configuration"), QSqlError::StatementError, _drv));
        return false;
    }

    r = SQLPrepare(hStmt, detail::toSQLTCHAR(query).data(), (SQLINTEGER) query.length());

    if (r != SQL_SUCCESS)
    {
        _drv->setLastError(detail::qMakeError(QCoreApplication::translate("QODBCResult",
                     "Unable to prepare statement"), QSqlError::StatementError, _drv));
        return false;
    }
    return true;
}

bool Result::exec()
{
    using namespace detail;

    setActive(false);
    setAt(QSql::BeforeFirstRow);
    recInfo.clear();

    if (!hStmt)
    {
        qSqlWarning(QLatin1String("QODBCResult::exec: No statement handle available"), this);
        return false;
    }

    if (isSelect())
        SQLCloseCursor(hStmt);

    QVector<QVariant>& values = boundValues();
    QVector<QByteArray> tmpStorage(values.count(), QByteArray()); // holds temporary buffers
    QVarLengthArray<SQLLEN, 32> indicators(values.count());
    memset(indicators.data(), 0, indicators.size() * sizeof(SQLLEN));

    // bind parameters - only positional binding allowed
    int i;
    SQLRETURN r;
    for (i = 0; i < values.count(); ++i)
    {
        if (bindValueType(i) & QSql::Out)
            values[i].detach();
        const QVariant &val = values.at(i);
        SQLLEN *ind = &indicators[i];
        if (val.isNull())
            *ind = SQL_NULL_DATA;
        //QVariant a = val.userType();

//        if (val.userType() == qMetaTypeId<QUuidEx>())
//        {
//            val.setValue
//            const QUuidEx& uuid = val.value<QUuidEx>();
//            v = uuid.toRfc4122();
//        }
//        else if (val.userType() == qMetaTypeId<QUuid>())
//        {
//            const QUuid& uuid = val.value<QUuid>();
//            v = uuid.toRfc4122();
//        }

        switch (val.userType())
        {
            case QVariant::Date:
            {
                QByteArray &ba = tmpStorage[i];
                ba.resize(sizeof(DATE_STRUCT));
                DATE_STRUCT *dt = (DATE_STRUCT *)const_cast<char *>(ba.constData());
                QDate qdt = val.toDate();
                dt->year = qdt.year();
                dt->month = qdt.month();
                dt->day = qdt.day();
                r = SQLBindParameter(hStmt,
                                     i + 1,
                                     qParamType[bindValueType(i) & QSql::InOut],
                                     SQL_C_DATE, SQL_DATE,
                                     0,
                                     0,
                                     (void *) dt, 0, *ind == SQL_NULL_DATA ? ind : NULL);
                break;
            }
            case QVariant::Time:
            {
                QByteArray &ba = tmpStorage[i];
                ba.resize(sizeof(TIME_STRUCT));
                TIME_STRUCT *dt = (TIME_STRUCT *)const_cast<char *>(ba.constData());
                QTime qdt = val.toTime();
                dt->hour = qdt.hour();
                dt->minute = qdt.minute();
                dt->second = qdt.second();
                r = SQLBindParameter(hStmt, i + 1, qParamType[bindValueType(i) & QSql::InOut], SQL_C_TIME, SQL_TIME, 0, 0, (void *) dt, 0, *ind == SQL_NULL_DATA ? ind : NULL);
                break;
            }
            case QVariant::DateTime:
            {
                QByteArray &ba = tmpStorage[i];
                ba.resize(sizeof(TIMESTAMP_STRUCT));
                TIMESTAMP_STRUCT *dt = reinterpret_cast<TIMESTAMP_STRUCT *>(const_cast<char *>(ba.constData()));
                const QDateTime qdt = val.toDateTime();
                const QDate qdate = qdt.date();
                const QTime qtime = qdt.time();
                dt->year = qdate.year();
                dt->month = qdate.month();
                dt->day = qdate.day();
                dt->hour = qtime.hour();
                dt->minute = qtime.minute();
                dt->second = qtime.second();
                // (20 includes a separating period)
                const int precision =  _drv->datetimePrecision - 20;
                if (precision <= 0)
                {
                    dt->fraction = 0;
                }
                else
                {
                    dt->fraction = qtime.msec() * 1000000;

                    // (How many leading digits do we want to keep?  With SQL Server 2005, this should be 3: 123000000)
                    int keep = (int)qPow(10.0, 9 - qMin(9, precision));
                    dt->fraction = (dt->fraction / keep) * keep;
                }

                r = SQLBindParameter(hStmt,
                                     i + 1,
                                     qParamType[bindValueType(i) & QSql::InOut],
                                     SQL_C_TIMESTAMP,
                                     SQL_TIMESTAMP,
                                     _drv->datetimePrecision, precision, (void *) dt,
                                     0,
                                     *ind == SQL_NULL_DATA ? ind : NULL);
                break; }
            case QVariant::Int:
                r = SQLBindParameter(hStmt,
                                     i + 1,
                                     qParamType[bindValueType(i) & QSql::InOut],
                                     SQL_C_SLONG,
                                     SQL_INTEGER,
                                     0,
                                     0,
                                     const_cast<void *>(val.constData()),
                                     0,
                                     *ind == SQL_NULL_DATA ? ind : NULL);
                break;
            case QVariant::UInt:
                r = SQLBindParameter(hStmt,
                                      i + 1,
                                      qParamType[bindValueType(i) & QSql::InOut],
                                      SQL_C_ULONG,
                                      SQL_NUMERIC,
                                      15,
                                      0,
                                      const_cast<void *>(val.constData()),
                                      0,
                                      *ind == SQL_NULL_DATA ? ind : NULL);
                break;
            case QVariant::Double:
                r = SQLBindParameter(hStmt,
                                      i + 1,
                                      qParamType[bindValueType(i) & QSql::InOut],
                                      SQL_C_DOUBLE,
                                      SQL_DOUBLE,
                                      0,
                                      0,
                                      const_cast<void *>(val.constData()),
                                      0,
                                      *ind == SQL_NULL_DATA ? ind : NULL);
                break;
            case QVariant::LongLong:
                r = SQLBindParameter(hStmt,
                                      i + 1,
                                      qParamType[bindValueType(i) & QSql::InOut],
                                      SQL_C_SBIGINT,
                                      SQL_BIGINT,
                                      0,
                                      0,
                                      const_cast<void *>(val.constData()),
                                      0,
                                      *ind == SQL_NULL_DATA ? ind : NULL);
                break;
            case QVariant::ULongLong:
                r = SQLBindParameter(hStmt,
                                      i + 1,
                                      qParamType[bindValueType(i) & QSql::InOut],
                                      SQL_C_UBIGINT,
                                      SQL_BIGINT,
                                      0,
                                      0,
                                      const_cast<void *>(val.constData()),
                                      0,
                                      *ind == SQL_NULL_DATA ? ind : NULL);
                break;
            case QVariant::ByteArray:
            {
                if (*ind != SQL_NULL_DATA)
                {
                    *ind = val.toByteArray().size();
                }
                r = SQLBindParameter(hStmt,
                                      i + 1,
                                      qParamType[bindValueType(i) & QSql::InOut],
                                      SQL_C_BINARY,
                                      SQL_LONGVARBINARY,
                                      val.toByteArray().size(),
                                      0,
                                      const_cast<char *>(val.toByteArray().constData()),
                                      val.toByteArray().size(),
                                      ind);
                break;
            }
            //case 1755: /* QVariant::Type(qMetaTypeId<QUuidEx>()) */
            //case QVariant::Type(qMetaTypeId<QUuidEx>()): /* QVariant::Type(qMetaTypeId<QUuidEx>()) */
            //case qMetaTypeId<QUuidEx>().id:
            case QVariant::Uuid:
            {
                //constexpr int test_enum = qMetaTypeId<QUuidEx>();

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
                             .arg(addrToNumber(hStmt))
                             .arg(transactId());
                    setLastError(detail::qMakeError(QCoreApplication::translate("QODBCResult",
                                 msg.toStdString().data()), QSqlError::StatementError, _drv));

                    rollbackInternalTransact();
                    return false;
                }

                QByteArray ba;
                QDataStream streamVMan(&ba, QIODevice::WriteOnly);
                streamVMan << ((quint32*) v.data())[0];
                streamVMan << ((quint16*) v.data())[2];
                streamVMan << ((quint16*) v.data())[3];
                streamVMan << (quint64)bswap_64(((quint64*) v.data())[1]);

                if (*ind != SQL_NULL_DATA)
                {
                    int s = ba.size();
                    *ind = s;
                }

                r = SQLBindParameter(hStmt, i + 1, qParamType[bindValueType(i) & QSql::InOut], SQL_C_GUID, SQL_GUID, ba.size(), 0, const_cast<char *>(ba.constData()), ba.size(), ind);
                break;
            }
            case QVariant::Bool:
                r = SQLBindParameter(hStmt,
                                      i + 1,
                                      qParamType[bindValueType(i) & QSql::InOut],
                                      SQL_C_BIT,
                                      SQL_BIT,
                                      0,
                                      0,
                                      const_cast<void *>(val.constData()),
                                      0,
                                      *ind == SQL_NULL_DATA ? ind : NULL);
                break;
            case QVariant::String:
                if (_drv->unicode)
                {
                    QByteArray &ba = tmpStorage[i];
                    QString str = val.toString();
                    if (*ind != SQL_NULL_DATA)
                        *ind = str.length() * sizeof(SQLTCHAR);
                    int strSize = str.length() * sizeof(SQLTCHAR);

                    if (bindValueType(i) & QSql::Out)
                    {
                        const QVarLengthArray<SQLTCHAR> a(toSQLTCHAR(str));
                        ba = QByteArray((const char *)a.constData(), a.size() * sizeof(SQLTCHAR));
                        r = SQLBindParameter(hStmt,
                                            i + 1,
                                            qParamType[bindValueType(i) & QSql::InOut],
                                            SQL_C_TCHAR,
                                            strSize > 254 ? SQL_WLONGVARCHAR : SQL_WVARCHAR,
                                            0, // god knows... don't change this!
                                            0,
                                            ba.data(),
                                            ba.size(),
                                            ind);
                        break;
                    }
                    ba = QByteArray ((const char *)toSQLTCHAR(str).constData(), str.size()*sizeof(SQLTCHAR));
                    r = SQLBindParameter(hStmt,
                                          i + 1,
                                          qParamType[bindValueType(i) & QSql::InOut],
                                          SQL_C_TCHAR,
                                          strSize > 254 ? SQL_WLONGVARCHAR : SQL_WVARCHAR,
                                          strSize,
                                          0,
                                          const_cast<char *>(ba.constData()),
                                          ba.size(),
                                          ind);
                    break;
                }
                else
                {
                    QByteArray &str = tmpStorage[i];
                    str = val.toString().toUtf8();
                    if (*ind != SQL_NULL_DATA)
                        *ind = str.length();
                    int strSize = str.length();

                    r = SQLBindParameter(hStmt,
                                          i + 1,
                                          qParamType[bindValueType(i) & QSql::InOut],
                                          SQL_C_CHAR,
                                          strSize > 254 ? SQL_LONGVARCHAR : SQL_VARCHAR,
                                          strSize,
                                          0,
                                          const_cast<char *>(str.constData()),
                                          strSize,
                                          ind);
                    break;
                }
            // fall through
            default:
            {
                QByteArray &ba = tmpStorage[i];
                if (*ind != SQL_NULL_DATA)
                    *ind = ba.size();
                r = SQLBindParameter(hStmt,
                                      i + 1,
                                      qParamType[bindValueType(i) & QSql::InOut],
                                      SQL_C_BINARY,
                                      SQL_VARBINARY,
                                      ba.length() + 1,
                                      0,
                                      const_cast<char *>(ba.constData()),
                                      ba.length() + 1,
                                      ind);
                break;
            }
        }
        if (r != SQL_SUCCESS)
        {
            log_warn_m << "QODBCResult::exec: unable to bind variable:" << qODBCWarn(hStmt);
            setLastError(detail::qMakeError(QCoreApplication::translate("QODBCResult",
                         "Unable to bind variable"), QSqlError::StatementError, _drv));
            return false;
        }
    }
    r = SQLExecute(hStmt);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO && r != SQL_NO_DATA)
    {
        log_warn_m << "QODBCResult::exec: Unable to execute statement:" << qODBCWarn(hStmt);
        setLastError(detail::qMakeError(QCoreApplication::translate("QODBCResult",
                     "Unable to execute statement"), QSqlError::StatementError, _drv));
        return false;
    }

    SQLULEN isScrollable = 0;
    r = SQLGetStmtAttr(hStmt, SQL_ATTR_CURSOR_SCROLLABLE, &isScrollable, SQL_IS_INTEGER, 0);
    if(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
        setForwardOnly(isScrollable == SQL_NONSCROLLABLE);

    SQLSMALLINT count = 0;
    SQLNumResultCols(hStmt, &count);
    if (count)
    {
        setSelect(true);
        init(count);

        for (int i = 0; i < count; ++i)
        {
            recInfo.append(qMakeFieldInfo(this, i));
        }
    }
    else
    {
        setSelect(false);
    }
    setActive(true);

    //get out parameters
    if (!hasOutValues())
        return true;

    for (i = 0; i < values.count(); ++i)
    {
        switch (values.at(i).userType())
        {
            case QVariant::Date:
            {
                DATE_STRUCT ds = *((DATE_STRUCT *)const_cast<char *>(tmpStorage.at(i).constData()));
                values[i] = QVariant(QDate(ds.year, ds.month, ds.day));
                break;
            }
            case QVariant::Time: {
                TIME_STRUCT dt = *((TIME_STRUCT *)const_cast<char *>(tmpStorage.at(i).constData()));
                values[i] = QVariant(QTime(dt.hour, dt.minute, dt.second));
                break; }
            case QVariant::DateTime: {
                TIMESTAMP_STRUCT dt = *((TIMESTAMP_STRUCT*)
                                        const_cast<char *>(tmpStorage.at(i).constData()));
                values[i] = QVariant(QDateTime(QDate(dt.year, dt.month, dt.day),
                               QTime(dt.hour, dt.minute, dt.second, dt.fraction / 1000000)));
                break; }
            case QVariant::Bool:
            case QVariant::Int:
            case QVariant::UInt:
            case QVariant::Double:
            case QVariant::ByteArray:
            case QVariant::LongLong:
            case QVariant::ULongLong:
                //nothing to do
                break;
            case QVariant::String:
                if (_drv->unicode) {
                    if (bindValueType(i) & QSql::Out)
                    {
                        const QByteArray &first = tmpStorage.at(i);
                        QVarLengthArray<SQLTCHAR> array;
                        array.append((const SQLTCHAR *)first.constData(), first.size());
                        values[i] = fromSQLTCHAR(array, first.size()/sizeof(SQLTCHAR));
                    }
                    break;
                }
                // fall through
            default:
            {
                if (bindValueType(i) & QSql::Out)
                    values[i] = tmpStorage.at(i);
                break;
            }
        }
        if (indicators[i] == SQL_NULL_DATA)
            values[i] = QVariant(QVariant::Type(values[i].userType()));
    }

    return true;
}

bool Result::gotoNext(SqlCachedResult::ValueCache& row, int rowIdx)
{
    using namespace  detail;
    SQLRETURN r(0);

    if (_drv->hasSQLFetchScroll)
        r = SQLFetchScroll(hStmt, SQL_FETCH_NEXT, 0);
    else
        r = SQLFetch(hStmt);

    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        if (r != SQL_NO_DATA)
            setLastError(detail::qMakeError(QCoreApplication::translate("QODBCResult", "Unable to fetch next"), QSqlError::ConnectionError, this->_drv));
        return false;
    }

    int colCnt = colCount();

    for (int i = 0; i < colCnt; ++i)
    {
        int idx = rowIdx + i;

        QByteArray fieldVal;
        SQLSMALLINT colNameLen;
        SQLSMALLINT colType;
        SQLULEN colSize;
        SQLSMALLINT colScale;
        SQLSMALLINT nullable;
        SQLLEN lengthIndicator = 0;
        SQLRETURN r = SQL_ERROR;

        QVarLengthArray<SQLTCHAR> colName(COLNAMESIZE);

        r = SQLDescribeCol(hStmt, i+1, colName.data(), COLNAMESIZE, &colNameLen, &colType, &colSize, &colScale, &nullable);
        // some servers do not support fetching column n after we already
        // fetched column n+1, so cache all previous columns here
        //const QSqlField info = rInf.field(i);
        switch (colType)
        {

            case SQL_BIGINT:
                row[idx] = qGetBigIntData(hStmt, i);
                break;
            case SQL_INTEGER:
                row[idx] = qGetIntData(hStmt, i+1);
                break;
            case SQL_DATE:
                DATE_STRUCT dbuf;
                r = SQLGetData(hStmt, i + 1, SQL_C_DATE, (SQLPOINTER)&dbuf, 0, &lengthIndicator);
                if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && (lengthIndicator != SQL_NULL_DATA))
                    row[idx] = QVariant(QDate(dbuf.year, dbuf.month, dbuf.day));
                else
                    row[idx] = QVariant(QVariant::Date);
            break;
            case QVariant::Time:
                TIME_STRUCT tbuf;
                r = SQLGetData(hStmt, i + 1, SQL_C_TIME, (SQLPOINTER)&tbuf, 0, &lengthIndicator);
                if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && (lengthIndicator != SQL_NULL_DATA))
                    row[idx] = QVariant(QTime(tbuf.hour, tbuf.minute, tbuf.second));
                else
                    row[idx] = QVariant(QVariant::Time);
            break;
            case SQL_TIMESTAMP:
                TIMESTAMP_STRUCT dtbuf;
                r = SQLGetData(hStmt, i + 1, SQL_C_TIMESTAMP, (SQLPOINTER)&dtbuf, 0, &lengthIndicator);
                if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && (lengthIndicator != SQL_NULL_DATA))
                    row[idx] = QVariant(QDateTime(QDate(dtbuf.year, dtbuf.month, dtbuf.day),
                           QTime(dtbuf.hour, dtbuf.minute, dtbuf.second, dtbuf.fraction / 1000000)));
                else
                    row[idx] = QVariant(QVariant::DateTime);
                break;
            case SQL_GUID:
            {
                QUuid uuid = qGetGuidData(hStmt, i).toUuid();
                const QUuidEx& uuidex = static_cast<const QUuidEx&>(uuid);
                row[idx] = uuidex;
                break;
            }
            case SQL_VARBINARY:
            case SQL_BINARY:
                row[idx] = qGetBinaryData(hStmt, i);
                break;
            case SQL_LONGVARCHAR:
            case SQL_CHAR:
                row[idx] = qGetStringData(hStmt, i, colSize, _drv->unicode);
                break;
            case SQL_DOUBLE:
                switch(numericalPrecisionPolicy())
                {
                    case QSql::LowPrecisionInt32:
                         row[idx] = qGetIntData(hStmt, i);
                        break;
                    case QSql::LowPrecisionInt64:
                         row[idx] = qGetBigIntData(hStmt, i);
                        break;
                    case QSql::LowPrecisionDouble:
                         row[idx] = qGetDoubleData(hStmt, i);
                        break;
                    case QSql::HighPrecision:
                         row[idx] = qGetStringData(hStmt, i, colSize, false);
                        break;
                }
                break;
            default:
                row[idx] = QVariant(qGetStringData(hStmt, i, colSize, false));
                break;
        }
    }

    return true;
}

bool Result::reset(const QString& query)
{
    using namespace detail;

    setActive(false);
    setAt(QSql::BeforeFirstRow);
    recInfo.clear();

    // Always reallocate the statement handle - the statement attributes
    // are not reset if SQLFreeStmt() is called which causes some problems.
    SQLRETURN r;
    if (hStmt && isStmtHandleValid())
    {
        r = SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
        if (r != SQL_SUCCESS)
        {
            qSqlWarning(QLatin1String("QODBCResult::reset: Unable to free statement handle"), this);
            return false;
        }
    }
    r  = SQLAllocHandle(SQL_HANDLE_STMT, _drv->hDbc, &hStmt);
    if (r != SQL_SUCCESS)
    {
        qSqlWarning(QLatin1String("QODBCResult::reset: Unable to allocate statement handle"), this);
        return false;
    }

    updateStmtHandleState();

    if (isForwardOnly())
    {
        r = SQLSetStmtAttr(hStmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, SQL_IS_UINTEGER);
    }
    else
    {
        r = SQLSetStmtAttr(hStmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_STATIC, SQL_IS_UINTEGER);
    }
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        setLastError(qMakeError(QCoreApplication::translate("QODBCResult",
            "QODBCResult::reset: Unable to set 'SQL_CURSOR_STATIC' as statement attribute. "
            "Please check your ODBC driver configuration"), QSqlError::StatementError, this->_drv));
        return false;
    }

    r = SQLExecDirect(hStmt, toSQLTCHAR(query).data(), (SQLINTEGER) query.length());
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO && r!= SQL_NO_DATA)
    {
        setLastError(qMakeError(QCoreApplication::translate("QODBCResult",
                     "Unable to execute statement"), QSqlError::StatementError, this->_drv));
        return false;
    }

    SQLULEN isScrollable = 0;
    r = SQLGetStmtAttr(hStmt, SQL_ATTR_CURSOR_SCROLLABLE, &isScrollable, SQL_IS_INTEGER, 0);
    if(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
        setForwardOnly(isScrollable == SQL_NONSCROLLABLE);

    SQLSMALLINT count = 0;
    SQLNumResultCols(hStmt, &count);
    if (count)
    {
        setSelect(true);
        for (int i = 0; i < count; ++i)
        {
            recInfo.append(qMakeFieldInfo(this, i));
        }
    }
    else
    {
        setSelect(false);
    }
    setActive(true);

    return true;
}

int Result::size()
{
    return -1;
}

int Result::numRowsAffected()
{
    SQLLEN affectedRowCount = 0;
    SQLRETURN r = SQLRowCount(hStmt, &affectedRowCount);
    if (r == SQL_SUCCESS)
        return affectedRowCount;
    else
        detail::qSqlWarning(QLatin1String("QODBCResult::numRowsAffected: Unable to count affected rows"), this->_drv);

    return -1;
}

QSqlRecord Result::record() const
{
    if (!isActive() || !isSelect())
        return QSqlRecord();

    return recInfo;
}

void Result::clearValues()
{
}

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

static size_t qGetODBCVersion(const QString &connOpts)
{
    if (connOpts.contains(QLatin1String("SQL_ATTR_ODBC_VERSION=SQL_OV_ODBC3"), Qt::CaseInsensitive))
        return SQL_OV_ODBC3;
    return SQL_OV_ODBC2;
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

    SQLRETURN r;

    r = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &hEnv);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        detail::qSqlWarning(QLatin1String("QODBCDriver::open: Unable to allocate environment"), this);
        setOpenError(true);
        return false;
    }

    r = SQLSetEnvAttr(hEnv,
                      SQL_ATTR_ODBC_VERSION,
                      (SQLPOINTER)SQL_OV_ODBC3,
                      SQL_IS_UINTEGER);

    r = SQLAllocHandle(SQL_HANDLE_DBC, hEnv, &hDbc);

    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        detail::qSqlWarning(QLatin1String("QODBCDriver::open: Unable to allocate connection"), this);
        setOpenError(true);
        //cleanup();
        return false;
    }

    // Create the connection string
    QString connQStr;

    if (db.contains(QLatin1String(".dsn"), Qt::CaseInsensitive))
        connQStr = QLatin1String("FILEDSN=") + db;
    else if (db.contains(QLatin1String("DRIVER="), Qt::CaseInsensitive)
            || db.contains(QLatin1String("SERVER="), Qt::CaseInsensitive))
        connQStr = db;
    else
        connQStr = QLatin1String("DSN=") + db;

    if (!user.isEmpty())
        connQStr += QLatin1String(";UID=") + user;
    if (!password.isEmpty())
        connQStr += QLatin1String(";PWD=") + password;

    SQLSMALLINT cb;
    QVarLengthArray<SQLTCHAR> connOut(1024);
    memset(connOut.data(), 0, connOut.size() * sizeof(SQLTCHAR));
    r = SQLDriverConnect(hDbc, NULL, detail::toSQLTCHAR(connQStr).data(), (SQLSMALLINT)connQStr.length(), connOut.data(), 1024, &cb, /*SQL_DRIVER_NOPROMPT*/0);

    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        setLastError(detail::qMakeError(tr("Unable to connect"), QSqlError::ConnectionError, this));
        setOpenError(true);
        return false;
    }

//    if (!checkDriver())
//    {
//        //setLastError(qMakeError(tr("Unable to connect - Driver doesn't support all functionality required"), QSqlError::ConnectionError, this));
//        setOpenError(true);
//        //cleanup();
//        return false;
//    }

//    d->checkUnicode();
//    d->checkSchemaUsage();
//    d->checkDBMS();
//    d->checkHasSQLFetchScroll();
//    d->checkHasMultiResults();
//    d->checkDateTimePrecision();
    setOpen(true);
    setOpenError(false);
    if (dbmsType() == MSSqlServer)
    {
        QSqlQuery i(createResult());
        i.exec(QLatin1String("SET QUOTED_IDENTIFIER ON"));
    }

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

    if (hDbc)
        SQLDisconnect(hDbc);


    hDbc = 0;
    _threadId = 0;
    _transactAddr = 0;

    setOpen(false);
    setOpenError(false);

    log_verbose_m << "Database is closed. Connect: " << addrToNumber(hDbc);
}

bool Driver::isOpen() const
{
    return hDbc;
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
                     << ". Connect: " << addrToNumber(hDbc);
    }
    else
        log_warn_m << "Failed capture transaction address: "  << addrToNumber(transact)
                   << ". Already captured: " << addrToNumber(_transactAddr)
                   << ". Connect: " << addrToNumber(hDbc);
}

void Driver::releaseTransactAddr(Transaction* transact)
{
    if (_transactAddr == transact)
    {
        _transactAddr = 0;
        log_debug2_m << "Transaction address released: " << addrToNumber(transact)
                     << ". Connect: " << addrToNumber(hDbc);
    }
    else
        log_warn_m << "Failed release transaction address: "  << addrToNumber(transact)
                   << ". Already captured: " << addrToNumber(_transactAddr)
                   << ". Connect: " << addrToNumber(hDbc);
}

bool Driver::transactAddrIsEqual(Transaction* transact)
{
    return (_transactAddr == transact);
}

QString Driver::formatValue(const QSqlField& field, bool trimStrings) const
{
    return QSqlDriver::formatValue(field, trimStrings);
}

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

void Driver::abortOperation(/*const SQLHANDLE hStmt*/)
{
    log_verbose_m << "Abort sql-operation"
                  << ". Connect: " << addrToNumber(hDbc)
                  << " (call from thread: " << trd::gettid() << ")";

    _operationIsAborted = true;

    //if (SQLCancel(hStmt)) // SQLDisconnect
    {
        const int errBuffSize = 256;
        char errBuff[errBuffSize] = {0};
        {
            const char* msg = "Failed abort sql-operation";
            setLastError(QSqlError("PostgresDriver", msg, QSqlError::UnknownError, "1"));

            log_error_m << msg << "; Detail: " << errBuff;
        }
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

} // namespace mssql
} // namespace db

#undef log_error_m
#undef log_warn_m
#undef log_info_m
#undef log_verbose_m
#undef log_debug_m
#undef log_debug2_m
