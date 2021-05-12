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

#ifndef SQL_SS_TIME2
#define SQL_SS_TIME2 -154
#endif

#ifndef SQL_SS_TIMESTAMPOFFSET
#define SQL_SS_TIMESTAMPOFFSET -155
#endif

#define PRINT_ERROR(MSG, SOURCE) \
    detail::qSqlWarning(MSG, SOURCE, __func__, __LINE__)

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

QString fromSQLTCHAR(const QVarLengthArray<ushort>& input, int size=-1)
{
   QString result;

   // Удаление \0, так как некоторые драйверы ошибочно добавляют один
   int realsize = qMin(size, input.size());
   if (realsize > 0 && input[realsize-1] == 0)
       realsize--;

   result = QString::fromUtf16((const ushort *)input.constData(), realsize);

   return result;
}

QVarLengthArray<ushort> toSQLTCHAR(const QString &input)
{
    QVarLengthArray<ushort> result;
    result.resize(input.size());

    memcpy(result.data(), input.unicode(), input.size() * 2);
    result.append(0);

    return result;
}

QString qWarnODBCHandle(int handleType, SQLHANDLE handle, int* nativeCode = 0)
{
    SQLINTEGER errCode = 0; // Нативный код ошибки, возвращаемый функцией SQLGetDiagRecW
    SQLSMALLINT msgLen = 0;
    SQLRETURN r = SQL_NO_DATA;
    ushort state[SQL_SQLSTATE_SIZE+1];
    QVarLengthArray<ushort> description(SQL_MAX_MESSAGE_LENGTH);
    QString result;
    int i = 1;

    description[0] = 0;
    do
    {
        r = SQLGetDiagRecW(handleType, handle, i, state, &errCode, 0, 0, &msgLen);
        if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && msgLen > 0)
            description.resize(msgLen+1);

        r = SQLGetDiagRecW(handleType, handle, i, state, &errCode, description.data(), description.size(), &msgLen);
        if (r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
        {
            if (nativeCode && errCode)
                *nativeCode = errCode;

            const QString tmpstore = fromSQLTCHAR(description, msgLen);

            if (result != tmpstore)
            {
                if (!result.isEmpty())
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

QString qODBCWarn(const SQLHANDLE stmt, const SQLHANDLE envHandle = 0, const SQLHANDLE pDbC = 0, int* nativeCode = 0)
{
    QString result;

    if (stmt)
    {
        const QString hMessage = qWarnODBCHandle(SQL_HANDLE_STMT, stmt, nativeCode);
        if (!hMessage.isEmpty())
        {
            if (!result.isEmpty())
                result += QLatin1Char(' ');
            result += hMessage;
        }
    }

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

    return result;
}

QString qODBCWarn(const Result* result, int* nativeCode)
{
    return qODBCWarn(result->_stmt, result->_drv->_env, result->_drv->_dbc, nativeCode);
}

void qSqlWarning(const QString& message, const Result* result, const char* func, int line)
{
    alog::logger().error(alog::detail::file_name(__FILE__), func, line, "MssqlDrv")
        << message
        << ". Transact: " << addrToNumber(result->_drv->_dbc) << "/" << result->transactId()
        << ". Detail: "   << qODBCWarn((SQLHANDLE)result, 0);
}

void qSqlWarning(const QString& message, const Driver* driver, const char* func, int line)
{
    alog::logger().error(alog::detail::file_name(__FILE__), func, line, "MssqlDrv")
        << message
        << ". Transact: " << addrToNumber(driver->_dbc)
        << ". Detail: "   << qODBCWarn((SQLHANDLE)driver, 0);
}

void qSqlWarning(const QString& message, const SQLHANDLE stmt, const char* func, int line)
{
    alog::logger().error(alog::detail::file_name(__FILE__), func, line, "MssqlDrv")
        << message
        << ". Detail: " << qODBCWarn((SQLHANDLE)stmt);
}

QSqlError qMakeError(const QString& err, QSqlError::ErrorType type, const Driver* driver)
{
    int nativeCode = -1;
    QString message = qODBCWarn((SQLHANDLE)driver, &nativeCode);

    return QSqlError("QODBC3: " + err, message, type, nativeCode != -1 ? QString::number(nativeCode) : QString());
}

QVariant::Type qDecodeODBCType(SQLSMALLINT sqltype, bool isSigned = true)
{
    QVariant::Type type = QVariant::Invalid;

    switch (sqltype)
    {
        case SQL_FLOAT: // [float]
            type = QVariant::Type(qMetaTypeId<float>());
            break;
        case SQL_DECIMAL: // [decimal](18, 0)
        case SQL_NUMERIC: // [numeric](18, 0)
        case SQL_REAL: // [real]
            type = QVariant::Double;
            break;
        case SQL_SMALLINT: // [smallint]
            type = isSigned ? QVariant::Int : QVariant::UInt;
            break;
        case SQL_INTEGER: // [int]
            type = isSigned ? QVariant::Int : QVariant::UInt;
            break;
        case SQL_BIT: // [bit]
            type = QVariant::Bool;
        break;
        case SQL_TINYINT: // [tinyint]
            type = QVariant::Char;
            break;
        case SQL_BIGINT: // [bigint]
            type = isSigned ? QVariant::LongLong : QVariant::ULongLong;
            break;
        case SQL_BINARY:  // [binary](n), [timestamp]
        case SQL_VARBINARY: // [varbinary](n), [varbinary](max)
            type = QVariant::ByteArray;
            break;
        case SQL_TYPE_DATE: // [date]
            type = QVariant::Date;
            break;
        case SQL_SS_TIME2: // [time](7)
            type = QVariant::Time;
            break;
        case SQL_TYPE_TIMESTAMP: // [datetime], [datetime2](7), [smalldatetime]
        case SQL_SS_TIMESTAMPOFFSET: // [datetimeoffset](7)
            type = QVariant::DateTime;
            break;
        case SQL_WCHAR: // [nchar](10)
        case SQL_WVARCHAR: // [nvarchar](50), [nvarchar](max)
        case SQL_CHAR:  // [char](n)
        case SQL_VARCHAR: // [varchar](n),
            type = QVariant::String;
            break;
    #if (ODBCVER >= 0x0350)
        case SQL_GUID:
            type = QVariant::Type(qMetaTypeId<QUuidEx>());
            break;
    #endif
        default:
            type = QVariant::Invalid;
            log_warn_m << "qDecodeODBCType(): unknown datatype: " << type;
            break;
    }

    return type;
}

QString qGetStringData(SQLHANDLE stmt, int column, int colSize, bool unicode = false)
{
    QString fieldVal;
    SQLRETURN r = SQL_ERROR;
    SQLLEN lengthIndicator = 0;

    if (colSize <= 0)
    {
        colSize = 256;
    }
    else if (colSize > 65536)
    {
        colSize = 65536;
    }
    else
    {
        colSize++; // make sure there is room for more than the 0 termination
    }
    if (unicode)
    {
        r = SQLGetData(stmt, column+1, SQL_C_TCHAR, NULL, 0, &lengthIndicator);
        if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && lengthIndicator > 0)
            colSize = int(lengthIndicator / sizeof(ushort) + 1);

        QVarLengthArray<ushort> buf(colSize);
        memset(buf.data(), 0, colSize*sizeof(ushort));
        while (true)
        {
            r = SQLGetData(stmt, column+1, SQL_C_TCHAR, (SQLPOINTER)buf.data(), colSize*sizeof(ushort), &lengthIndicator);
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
                int rSize = (r == SQL_SUCCESS_WITH_INFO) ? colSize : int(lengthIndicator / sizeof(ushort));
                    fieldVal += fromSQLTCHAR(buf, rSize);
                if (lengthIndicator < SQLLEN(colSize*sizeof(ushort)))
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
                PRINT_ERROR("qGetStringData: Error while fetching data", stmt);

                fieldVal.clear();
                break;
            }
        }
    }
    else
    {
        r = SQLGetData(stmt, column+1, SQL_C_CHAR, NULL, 0, &lengthIndicator);
        if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && lengthIndicator > 0)
            colSize = lengthIndicator + 1;

        QVarLengthArray<SQLCHAR> buf(colSize);
        while (true)
        {
            r = SQLGetData(stmt, column+1, SQL_C_CHAR, (SQLPOINTER)buf.data(), colSize, &lengthIndicator);
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
                PRINT_ERROR("qGetStringData: Error while fetching data", stmt);
                fieldVal.clear();
                break;
            }
        }
    }
    return fieldVal;
}

QVariant qGetBinaryData(SQLHANDLE stmt, int column)
{
    QByteArray fieldVal;
    SQLSMALLINT colNameLen;
    SQLSMALLINT colType;
    SQLULEN colSize;
    SQLSMALLINT colScale;
    SQLSMALLINT nullable;
    SQLLEN lengthIndicator = 0;
    SQLRETURN r = SQL_ERROR;

    QVarLengthArray<ushort> colName(COLNAMESIZE);

    r = SQLDescribeColW(stmt, column + 1, colName.data(), COLNAMESIZE, &colNameLen, &colType, &colSize, &colScale, &nullable);
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
        r = SQLGetData(stmt, column+1, SQL_C_BINARY, const_cast<char *>(fieldVal.constData() + read), colSize, &lengthIndicator);
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

QVariant qGetGuidData(SQLHANDLE stmt, int column)
{
    QByteArray fieldVal;
    SQLSMALLINT colNameLen;
    SQLSMALLINT colType;
    SQLULEN colSize;
    SQLSMALLINT colScale;
    SQLSMALLINT nullable;
    SQLLEN lengthIndicator = 0;
    SQLRETURN r = SQL_ERROR;

    QVarLengthArray<ushort> colName(COLNAMESIZE);

    r = SQLDescribeColW(stmt, column + 1, colName.data(), COLNAMESIZE, &colNameLen, &colType, &colSize, &colScale, &nullable);
    if (r != SQL_SUCCESS)
        log_warn_m << "qGetGuidData: Unable to describe column" << column;

    // SQLDescribeCol may return 0 if size cannot be determined
    if (!colSize)
        return QVariant(QVariant::Uuid);

    colSize = 16;
    fieldVal.resize(colSize);

    r = SQLGetData(stmt, column+1, SQL_C_BINARY, const_cast<char *>(fieldVal.constData()), colSize, &lengthIndicator);
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

QVariant qGetIntData(SQLHANDLE stmt, int column, bool isSigned = true)
{
    SQLINTEGER intbuf = 0;
    SQLLEN lengthIndicator = 0;
    SQLRETURN r = SQLGetData(stmt, column+1, isSigned ? SQL_C_SLONG : SQL_C_ULONG, (SQLPOINTER)&intbuf, sizeof(intbuf), &lengthIndicator);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
        return QVariant(QVariant::Invalid);

    if (lengthIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::Int);

    if (isSigned)
        return int(intbuf);
    else
        return uint(intbuf);
}

QVariant qGetDoubleData(SQLHANDLE stmt, int column)
{
    SQLDOUBLE dblbuf;
    SQLLEN lengthIndicator = 0;
    SQLRETURN r = SQLGetData(stmt, column+1, SQL_C_DOUBLE, (SQLPOINTER) &dblbuf, 0, &lengthIndicator);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        return QVariant(QVariant::Invalid);
    }
    if (lengthIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::Double);

    return (double)dblbuf;
}

long strtohextoval(SQL_NUMERIC_STRUCT& numStruct)
{
    long value = 0;
    int i = 1, last = 1,current;
    int a = 0, b = 0;

    for (i = 0; i <= 15; i++)
    {
        current = (int)numStruct.val[i];
        a = current % 16;
        b = current / 16;

        value += last * a;
        last = last * 16;
        value += last * b;
        last = last * 16;
    }
    return value;
}

QVariant qGetNumericData(SQLHANDLE stmt, int column)
{
    SQLLEN lengthIndicator = 0;

    SQL_NUMERIC_STRUCT numStruct;
    SQLRETURN r = SQLGetData(stmt, column+1, SQL_C_NUMERIC, &numStruct, 19, &lengthIndicator);

    long divisor = 1;
    if (numStruct.scale > 0)
    {
        for (int i=0; i< numStruct.scale; i++)
            divisor = divisor * 10;
    }
    long myvalue;
    myvalue = strtohextoval(numStruct);

    double finalVal = (double)myvalue / divisor;

    int sign;

    if (!numStruct.sign)
        sign = -1;
    else
        sign =1;

    finalVal *= sign;

    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        return QVariant(QVariant::Invalid);
    }
    if (lengthIndicator == SQL_NULL_DATA)
        return QVariant::Type(qMetaTypeId<double>());

    return (double) finalVal;
}

QVariant qGetBigIntData(SQLHANDLE stmt, int column, bool isSigned = true)
{
    SQLBIGINT lngbuf = 0;
    SQLLEN lengthIndicator = 0;
    SQLRETURN r = SQLGetData(stmt, column+1, isSigned ? SQL_C_SBIGINT : SQL_C_UBIGINT, (SQLPOINTER) &lngbuf, sizeof(lngbuf), &lengthIndicator);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
        return QVariant(QVariant::Invalid);
    if (lengthIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::LongLong);

    if (isSigned)
        return qint64(lngbuf);
    else
        return quint64(lngbuf);
}

QVariant qGetBitData(SQLHANDLE stmt, int column)
{
    bool boolbuf = 0;

    SQLLEN lengthIndicator = 0;

    SQLRETURN r = SQLGetData(stmt, column+1, SQL_C_BIT, (SQLPOINTER) &boolbuf, sizeof(boolbuf), &lengthIndicator);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
        return QVariant(QVariant::Invalid);
    if (lengthIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::Bool);

    return (bool)boolbuf;
}

bool isAutoValue(const SQLHANDLE stmt, int column)
{
    SQLLEN nNumericAttribute = 0; // Check for auto-increment
    const SQLRETURN r = ::SQLColAttribute(stmt, column + 1, SQL_DESC_AUTO_UNIQUE_VALUE, 0, 0, 0, &nNumericAttribute);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        PRINT_ERROR("qMakeField: Unable to get autovalue attribute for column " + QString::number(column), stmt);
        return false;
    }
    return nNumericAttribute != SQL_FALSE;
}

// creates a QSqlField from a valid stmt generated
// by SQLColumns. The stmt has to point to a valid position.
QSqlField qMakeFieldInfo(const SQLHANDLE stmt, const DriverPtr& driver)
{
    QString fname = qGetStringData(stmt, 3, -1, driver->unicode);
    int type = qGetIntData(stmt, 4).toInt(); // column type
    QSqlField f(fname, qDecodeODBCType(type, driver));
    QVariant var = qGetIntData(stmt, 6);
    f.setLength(var.isNull() ? -1 : var.toInt()); // column size
    var = qGetIntData(stmt, 8).toInt();
    f.setPrecision(var.isNull() ? -1 : var.toInt()); // precision
    f.setSqlType(type);
    int required = qGetIntData(stmt, 10).toInt(); // nullable-flag
    // required can be SQL_NO_NULLS, SQL_NULLABLE or SQL_NULLABLE_UNKNOWN
    if (required == SQL_NO_NULLS)
        f.setRequired(true);
    else if (required == SQL_NULLABLE)
        f.setRequired(false);
    // else we don't know
    return f;
}

QSqlField qMakeFieldInfo(const Result* result, int i )
{
    QString errorMessage;
    const QSqlField field = qMakeFieldInfo(result->_stmt, i, &errorMessage);
    if (!errorMessage.isEmpty())
        PRINT_ERROR(errorMessage, result);

    return field;
}

QSqlField qMakeFieldInfo(const SQLHANDLE stmt, int i, QString* errorMessage)
{
    SQLSMALLINT colNameLen;
    SQLSMALLINT colType;
    SQLULEN colSize;
    SQLSMALLINT colScale;
    SQLSMALLINT nullable;
    SQLRETURN r = SQL_ERROR;
    QVarLengthArray<ushort> colName(COLNAMESIZE);
    errorMessage->clear();
    r = SQLDescribeColW(stmt, i+1, colName.data(), (SQLSMALLINT)COLNAMESIZE, &colNameLen, &colType, &colSize, &colScale, &nullable);

    if (r != SQL_SUCCESS)
    {
        *errorMessage = QStringLiteral("qMakeField: Unable to describe column ") + QString::number(i);
        return QSqlField();
    }

    SQLLEN unsignedFlag = SQL_FALSE;
    r = SQLColAttribute (stmt, i + 1, SQL_DESC_UNSIGNED, 0, 0, 0, &unsignedFlag);
    if (r != SQL_SUCCESS)
    {
        PRINT_ERROR("qMakeField: Unable to get column attributes for column " + QString::number(i), stmt);
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
    f.setAutoValue(isAutoValue(stmt, i));
    QVarLengthArray<ushort> tableName(TABLENAMESIZE);
    SQLSMALLINT tableNameLen;
    r = SQLColAttribute(stmt, i + 1, SQL_DESC_BASE_TABLE_NAME, tableName.data(), TABLENAMESIZE, &tableNameLen, 0);
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
    SQLRETURN r = SQLSetConnectAttr(_drv->_dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)size_t(ac), sizeof(ac));
    if (r != SQL_SUCCESS)
    {
        _drv->setLastError(detail::qMakeError("Unable to disable autocommit", QSqlError::TransactionError, _drv));
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
    SQLRETURN r = SQLEndTran(SQL_HANDLE_DBC, _drv->_dbc, SQL_COMMIT);
    if (r != SQL_SUCCESS)
    {
        _drv->setLastError(detail::qMakeError("Unable to commit transaction", QSqlError::TransactionError, _drv));
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
    SQLRETURN r = SQLEndTran(SQL_HANDLE_DBC, _drv->_dbc, SQL_ROLLBACK);
    if (r != SQL_SUCCESS)
    {
        _drv->setLastError(detail::qMakeError("Unable to rollback transaction", QSqlError::TransactionError, _drv));
        return false;
    }
    return endTrans();
}

bool Transaction::endTrans()
{
    SQLUINTEGER ac(SQL_AUTOCOMMIT_ON);
    SQLRETURN r = SQLSetConnectAttr(_drv->_dbc, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)size_t(ac), sizeof(ac));
    if (r != SQL_SUCCESS)
    {
        _drv->setLastError(detail::qMakeError("Unable to enable autocommit", QSqlError::TransactionError, _drv));
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

    chk_connect_d(_drv.get(), SIGNAL(abortStatement()), this, SLOT(abortStatement()));

}

Result::Result(const Transaction::Ptr& trans, ForwardOnly forwardOnly)
    : SqlCachedResult(trans->_drv.get()),
      _drv(trans->_drv),
      _externalTransact(trans)
{
    Q_ASSERT(_drv.get());
    setForwardOnly(forwardOnly == ForwardOnly::Yes);

    chk_connect_d(_drv.get(), SIGNAL(abortStatement()), this, SLOT(abortStatement()));
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
    log_debug2_m << "Begin dataset cleanup. Connect: " << addrToNumber(_stmt);

    if (!_externalTransact)
        if (_internalTransact && _internalTransact->isActive())
        {
             if (isSelectSql())
                 rollbackInternalTransact();
             else
                 commitInternalTransact();
        }

    SQLFreeStmt(_stmt, SQL_RESET_PARAMS);
    _stmt = nullptr;

    _preparedQuery.clear();

    SqlCachedResult::cleanup();

    log_debug2_m << "End dataset cleanup. Connect: " << addrToNumber(_stmt);
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
        _drv->setLastError(detail::qMakeError("Failed begin internal transaction", QSqlError::TransactionError, _drv));

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
        _drv->setLastError(detail::qMakeError("Failed commit internal transaction", QSqlError::TransactionError, _drv));
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
        _drv->setLastError(detail::qMakeError("Failed rollback internal transaction", QSqlError::TransactionError, _drv));
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

    _recInfo.clear();
    if (_stmt && isStmtHandleValid())
    {
        r = SQLFreeHandle(SQL_HANDLE_STMT, _stmt);
        if (r != SQL_SUCCESS)
        {
            PRINT_ERROR("QODBCResult::prepare: Unable to close statement", _stmt);
            return false;
        }
    }
    r = SQLAllocHandle(SQL_HANDLE_STMT, _drv->_dbc, &_stmt);
    if (r != SQL_SUCCESS)
    {
        PRINT_ERROR("QODBCResult::prepare: Unable to allocate statement handle", _stmt);
        return false;
    }

    updateStmtHandleState();

    if (isForwardOnly())
    {
        r = SQLSetStmtAttr(_stmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, SQL_IS_UINTEGER);
    }
    else
    {
        r = SQLSetStmtAttr(_stmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_STATIC, SQL_IS_UINTEGER);
    }
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        _drv->setLastError(detail::qMakeError(
            "QODBCResult"
            ". QODBCResult::reset: Unable to set 'SQL_CURSOR_STATIC' as statement attribute. "
            ". Please check your ODBC driver configuration", QSqlError::StatementError, _drv));
        return false;
    }

    r = SQLPrepareW(_stmt, detail::toSQLTCHAR(query).data(), (SQLINTEGER) query.length());

    if (r != SQL_SUCCESS)
    {
        _drv->setLastError(detail::qMakeError("QODBCResult. Unable to prepare statement", QSqlError::StatementError, _drv));
        return false;
    }

    _preparedQuery = query;

    return true;
}

bool Result::exec()
{
    using namespace detail;

    setActive(false);
    setAt(QSql::BeforeFirstRow);
    _recInfo.clear();

    if (!_stmt)
    {
        PRINT_ERROR("QODBCResult::exec: No statement handle available", this);
        return false;
    }

    if (isSelect())
        SQLCloseCursor(_stmt);

    QVector<QVariant>& values = boundValues();
    QVector<QByteArray> tmpStorage(values.count(), QByteArray()); // holds temporary buffers
    QVarLengthArray<SQLLEN, 32> indicators(values.count());
    memset(indicators.data(), 0, indicators.size() * sizeof(SQLLEN));

    // bind parameters - only positional binding allowed
    int i;
    SQLRETURN r;
    for (i = 0; i < values.count(); ++i)
    {
        SQLSMALLINT dataType, decimalDigits, nullable;
        SQLULEN bytesRemaining;

        r = SQLDescribeParam(_stmt, i + 1, &dataType, &bytesRemaining, &decimalDigits, &nullable);

        if (r != SQL_SUCCESS)
        {
            PRINT_ERROR("QODBCResult::exec: unable to bind variable", _stmt);
            setLastError(detail::qMakeError("QODBCResult. Unable to bind variable", QSqlError::StatementError, _drv));
            return false;
        }

        if (bindValueType(i) & QSql::Out)
            values[i].detach();
        const QVariant &val = values.at(i);
        SQLLEN *ind = &indicators[i];
        if (val.isNull())
            *ind = SQL_NULL_DATA;

        switch(dataType)
        {
            case SQL_TYPE_DATE: // [date]

            {
                QByteArray &ba = tmpStorage[i];
                ba.resize(sizeof(DATE_STRUCT));
                DATE_STRUCT *dt = (DATE_STRUCT *)const_cast<char *>(ba.constData());
                QDate qdt = val.toDate();
                dt->year = qdt.year();
                dt->month = qdt.month();
                dt->day = qdt.day();
                r = SQLBindParameter(_stmt,
                                     i + 1,
                                     qParamType[bindValueType(i) & QSql::InOut],
                                     SQL_C_DATE,
                                     SQL_DATE,
                                     0,
                                     0,
                                     (void *) dt,
                                     0,
                                     *ind == SQL_NULL_DATA ? ind : NULL);
                break;
            }
            case SQL_TYPE_TIME: // [time](7)
            case SQL_SS_TIME2: // [time](7)
            {
                QByteArray &ba = tmpStorage[i];
                ba.resize(sizeof(TIME_STRUCT));
                TIME_STRUCT *dt = (TIME_STRUCT *)const_cast<char *>(ba.constData());
                QTime qdt = val.toTime();
                dt->hour = qdt.hour();
                dt->minute = qdt.minute();
                dt->second = qdt.second();
                r = SQLBindParameter(_stmt,
                                     i + 1,
                                     qParamType[bindValueType(i) & QSql::InOut],
                                     SQL_C_TIME,
                                     SQL_TIME,
                                     0,
                                     0,
                                     (void *) dt,
                                     0,
                                     *ind == SQL_NULL_DATA ? ind : NULL);
                break;
            }
            case SQL_TYPE_TIMESTAMP: // [datetime], [datetime2](7), [smalldatetime]
            case SQL_SS_TIMESTAMPOFFSET: // [datetimeoffset](7)
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

                r = SQLBindParameter(_stmt,
                                     i + 1,
                                     qParamType[bindValueType(i) & QSql::InOut],
                                     SQL_C_TIMESTAMP,
                                     SQL_TIMESTAMP,
                                     _drv->datetimePrecision,
                                     dt->fraction,
                                     (void *) dt,
                                     0,
                                     *ind == SQL_NULL_DATA ? ind : NULL);
                break; }
            case SQL_SMALLINT: // [smallint]
                r = SQLBindParameter(_stmt,
                                     i + 1,
                                     qParamType[bindValueType(i) & QSql::InOut],
                                     SQL_C_USHORT,
                                     SQL_SMALLINT,
                                     0,
                                     0,
                                     const_cast<void *>(val.constData()),
                                     0,
                                     *ind == SQL_NULL_DATA ? ind : NULL);
                break;

            case SQL_INTEGER: // [int]
                r = SQLBindParameter(_stmt,
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
            case SQL_TINYINT: // [tinyint]
                r = SQLBindParameter(_stmt,
                                     i + 1,
                                     qParamType[bindValueType(i) & QSql::InOut],
                                     SQL_C_UTINYINT,
                                     SQL_TINYINT,
                                     15,
                                     0,
                                     const_cast<void *>(val.constData()),
                                     0,
                                     *ind == SQL_NULL_DATA ? ind : NULL);
                break;
            case SQL_DECIMAL: // [decimal](18, 0)
            case SQL_NUMERIC: // [numeric](18, 0)
            case SQL_REAL: // [real]
            case SQL_FLOAT: // [float]
                r = SQLBindParameter(_stmt,
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
            case SQL_BIGINT: // [bigint]
                r = SQLBindParameter(_stmt,
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
            case SQL_BINARY: // [binary](n)
            case SQL_VARBINARY: // [varbinary](n), [varbinary](max)
            case SQL_LONGVARBINARY: // !
            {
                if (*ind != SQL_NULL_DATA)
                {
                    *ind = val.toByteArray().size();
                }
                r = SQLBindParameter(_stmt,
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
            case SQL_GUID: // [uniqueidentifier]
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
                             .arg(addrToNumber(_stmt))
                             .arg(transactId());
                    setLastError(detail::qMakeError("QODBCResult. "+ QString(msg.toStdString().data()), QSqlError::StatementError, _drv));

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

                r = SQLBindParameter(_stmt,
                                     i + 1,
                                     qParamType[bindValueType(i) & QSql::InOut],
                                     SQL_C_GUID,
                                     SQL_GUID,
                                     ba.size(),
                                     0,
                                     const_cast<char *>(ba.constData()),
                                     ba.size(),
                                     ind);
                break;
            }
            case SQL_BIT: //[bit]
                r = SQLBindParameter(_stmt,
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
            case SQL_WCHAR: // [nchar](n)
            case SQL_WVARCHAR: // [nvarchar](n), [nvarchar](max)
            case SQL_CHAR: // [char](n)
            case SQL_VARCHAR: // [varchar](n), [varchar](max)
            case SQL_LONGVARCHAR:
            {
                QByteArray &ba = tmpStorage[i];
                QString str = val.toString();
                if (*ind != SQL_NULL_DATA)
                    *ind = str.length() * sizeof(ushort);
                int strSize = str.length() * sizeof(ushort);

                if (bindValueType(i) & QSql::Out)
                {
                    const QVarLengthArray<ushort> a(toSQLTCHAR(str));
                    ba = QByteArray((const char *)a.constData(), a.size() * sizeof(ushort));
                    r = SQLBindParameter(_stmt,
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
                ba = QByteArray ((const char *)toSQLTCHAR(str).constData(), str.size()*sizeof(ushort));
                r = SQLBindParameter(_stmt,
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
            // fall through
            default:
            {
                PRINT_ERROR("QODBCResult::exec: unsupported datatype: ", _stmt);

                QByteArray &ba = tmpStorage[i];
                if (*ind != SQL_NULL_DATA)
                    *ind = ba.size();
                r = SQLBindParameter(_stmt,
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
            PRINT_ERROR("QODBCResult::exec: unable to bind variable", _stmt);
            setLastError(detail::qMakeError("QODBCResult. Unable to bind variable", QSqlError::StatementError, _drv));
            return false;
        }
    }
    r = SQLExecute(_stmt);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO && r != SQL_NO_DATA)
    {
        PRINT_ERROR("QODBCResult::exec: unable to bind variable", _stmt);
        setLastError(detail::qMakeError("QODBCResult. Unable to execute statement", QSqlError::StatementError, _drv));
        return false;
    }

    SQLULEN isScrollable = 0;
    r = SQLGetStmtAttr(_stmt, SQL_ATTR_CURSOR_SCROLLABLE, &isScrollable, SQL_IS_INTEGER, 0);
    if(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
        setForwardOnly(isScrollable == SQL_NONSCROLLABLE);

    SQLSMALLINT count = 0;
    SQLNumResultCols(_stmt, &count);
    if (count)
    {
        setSelect(true);
        init(count);

        for (int i = 0; i < count; ++i)
        {
            _recInfo.append(qMakeFieldInfo(this, i));
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
            case QVariant::Time:
            {
                TIME_STRUCT dt = *((TIME_STRUCT *)const_cast<char *>(tmpStorage.at(i).constData()));
                values[i] = QVariant(QTime(dt.hour, dt.minute, dt.second));
                break;
            }
            case QVariant::DateTime:
            {
                TIMESTAMP_STRUCT dt = *((TIMESTAMP_STRUCT*)
                                        const_cast<char *>(tmpStorage.at(i).constData()));
                values[i] = QVariant(QDateTime(QDate(dt.year, dt.month, dt.day),
                               QTime(dt.hour, dt.minute, dt.second, dt.fraction / 1000000)));
                break;
            }
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
                        QVarLengthArray<ushort> array;
                        array.append((const ushort *)first.constData(), first.size());
                        values[i] = fromSQLTCHAR(array, first.size()/sizeof(ushort));
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

void Result::abortStatement()
{
    if (SQLCancel(_stmt))
    {
        const int errBuffSize = 256;
        char errBuff[errBuffSize] = {0};
        {
            const char* msg = "Failed abort sql-operation";
            setLastError(QSqlError("MSSQL Driver", msg, QSqlError::UnknownError, "Unable abort statement"));

            log_error_m << msg << "; Detail: " << errBuff;
        }
    }
}

bool Result::gotoNext(SqlCachedResult::ValueCache& row, int rowIdx)
{
    using namespace  detail;
    SQLRETURN r(0);

    if (_drv->hasSQLFetchScroll)
        r = SQLFetchScroll(_stmt, SQL_FETCH_NEXT, 0);
    else
        r = SQLFetch(_stmt);

    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        if (r != SQL_NO_DATA)
            setLastError(detail::qMakeError("QODBCResult. Unable to fetch next", QSqlError::ConnectionError, this->_drv));
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

        QVarLengthArray<ushort> colName(COLNAMESIZE);

        r = SQLDescribeColW(_stmt, i+1, colName.data(), COLNAMESIZE, &colNameLen, &colType, &colSize, &colScale, &nullable);
        if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
        {
            return false;
        }
        // some servers do not support fetching column n after we already
        // fetched column n+1, so cache all previous columns here
        //const QSqlField info = rInf.field(i);
        switch (colType)
        {
            case SQL_BIT: // [bit]
                 row[idx] = qGetBitData(_stmt, i);
            break;
            case SQL_BIGINT: // [bigint]
                row[idx] = qGetBigIntData(_stmt, i);
                break;
            case SQL_TINYINT: // [tinyint]
            case SQL_SMALLINT: // [smallint]
            case SQL_INTEGER: // [int]
                row[idx] = qGetIntData(_stmt, i);
                break;
            case SQL_TYPE_DATE: // [date]
                DATE_STRUCT dbuf;
                r = SQLGetData(_stmt, i + 1, SQL_C_DATE, (SQLPOINTER)&dbuf, 0, &lengthIndicator);
                if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && (lengthIndicator != SQL_NULL_DATA))
                    row[idx] = QVariant(QDate(dbuf.year, dbuf.month, dbuf.day));
                else
                    row[idx] = QVariant(QVariant::Date);
            break;
            case SQL_SS_TIME2: // [time](7)
                TIME_STRUCT tbuf;
                r = SQLGetData(_stmt, i + 1, SQL_C_TIME, (SQLPOINTER)&tbuf, 0, &lengthIndicator);
                if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && (lengthIndicator != SQL_NULL_DATA))
                    row[idx] = QVariant(QTime(tbuf.hour, tbuf.minute, tbuf.second));
                else
                    row[idx] = QVariant(QVariant::Time);
            break;
            case SQL_TYPE_TIMESTAMP: // [datetime], [datetime2](7), [smalldatetime]
            case SQL_SS_TIMESTAMPOFFSET: // [datetimeoffset](7)
                TIMESTAMP_STRUCT dtbuf;
                r = SQLGetData(_stmt, i + 1, SQL_C_TIMESTAMP, (SQLPOINTER)&dtbuf, 0, &lengthIndicator);
                if ((r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO) && (lengthIndicator != SQL_NULL_DATA))
                    row[idx] = QVariant(QDateTime(QDate(dtbuf.year, dtbuf.month, dtbuf.day),
                           QTime(dtbuf.hour, dtbuf.minute, dtbuf.second, dtbuf.fraction / 1000000)));
                else
                    row[idx] = QVariant(QVariant::DateTime);
                break;
            case SQL_GUID: // [uniqueidentifier]
            {
                QUuid uuid = qGetGuidData(_stmt, i).toUuid();
                const QUuidEx& uuidex = static_cast<const QUuidEx&>(uuid);
                row[idx] = uuidex;
                break;
            }
            case SQL_VARBINARY: // [varbinary](n), [varbinary](max)
            case SQL_BINARY: // [binary](n), [timestamp]
                row[idx] = qGetBinaryData(_stmt, i);
                break;
            case SQL_CHAR: // [char](n)
            case SQL_WCHAR: // [nchar](10)
            case SQL_WVARCHAR: // [nvarchar](50), [nvarchar](max)
            case SQL_VARCHAR: // [varchar](n)
                row[idx] = qGetStringData(_stmt, i, colSize, _drv->unicode);
                break;

        case SQL_NUMERIC: // [numeric](18, 0)
        case SQL_DECIMAL: // [decimal](18, 0)
        {
            row[idx] = qGetNumericData(_stmt, i);
           break;
        }

        case SQL_REAL: // [real]
        case SQL_FLOAT: // [float]
                switch(numericalPrecisionPolicy())
                {
                    case QSql::LowPrecisionInt32:
                         row[idx] = qGetIntData(_stmt, i);
                        break;
                    case QSql::LowPrecisionInt64:
                         row[idx] = qGetBigIntData(_stmt, i);
                        break;
                    case QSql::LowPrecisionDouble:
                         row[idx] = qGetDoubleData(_stmt, i);
                        break;
                    case QSql::HighPrecision:
                         row[idx] = qGetStringData(_stmt, i, colSize, false);
                        break;
                }
                break;
            default:
                row[idx] = QVariant(qGetStringData(_stmt, i, colSize, false));
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
    _recInfo.clear();

    // Always reallocate the statement handle - the statement attributes
    // are not reset if SQLFreeStmt() is called which causes some problems.
    SQLRETURN r;
    if (_stmt && isStmtHandleValid())
    {
        r = SQLFreeHandle(SQL_HANDLE_STMT, _stmt);
        if (r != SQL_SUCCESS)
        {
            PRINT_ERROR("QODBCResult::reset: Unable to free statement handle", this);
            return false;
        }
    }
    r  = SQLAllocHandle(SQL_HANDLE_STMT, _drv->_dbc, &_stmt);
    if (r != SQL_SUCCESS)
    {
        PRINT_ERROR("QODBCResult::reset: Unable to allocate statement handle", this);
        return false;
    }

    updateStmtHandleState();

    if (isForwardOnly())
    {
        r = SQLSetStmtAttr(_stmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, SQL_IS_UINTEGER);
    }
    else
    {
        r = SQLSetStmtAttr(_stmt, SQL_ATTR_CURSOR_TYPE, (SQLPOINTER)SQL_CURSOR_STATIC, SQL_IS_UINTEGER);
    }
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        setLastError(qMakeError("QODBCResult::reset: Unable to set 'SQL_CURSOR_STATIC' as statement attribute"
            ". Please check your ODBC driver configuration", QSqlError::StatementError, this->_drv));
        return false;
    }

    r = SQLExecDirectW(_stmt, toSQLTCHAR(query).data(), (SQLINTEGER) query.length());
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO && r!= SQL_NO_DATA)
    {
        setLastError(qMakeError("QODBCResult: Unable to execute statement", QSqlError::StatementError, this->_drv));
        return false;
    }

    SQLULEN isScrollable = 0;
    r = SQLGetStmtAttr(_stmt, SQL_ATTR_CURSOR_SCROLLABLE, &isScrollable, SQL_IS_INTEGER, 0);
    if(r == SQL_SUCCESS || r == SQL_SUCCESS_WITH_INFO)
        setForwardOnly(isScrollable == SQL_NONSCROLLABLE);

    SQLSMALLINT count = 0;
    SQLNumResultCols(_stmt, &count);
    if (count)
    {
        setSelect(true);
        for (int i = 0; i < count; ++i)
        {
            _recInfo.append(qMakeFieldInfo(this, i));
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

int Result::size2(const DriverPtr& drv) const
{
    if (!isSelectSql() || _preparedQuery.isEmpty())
    {
        log_error_m << "Size of result unavailable"
                    << ". Detail: Sql-statement not SELETC or not prepared";
        return -1;
    }

    if (_drv.get() == drv.get())
    {
        log_error_m << "Size of result unavailable"
                    << ". Detail: It is not allowed to use the same database connection";
        return -1;
    }

    int pos = _preparedQuery.indexOf("FROM", Qt::CaseInsensitive);
    if (pos == -1)
    {
        log_error_m << "Size of result unavailable"
                    << ". Detail: Sql-statement not contains 'FROM' keyword";
        return -1;
    }

    QString query = "SELECT COUNT(*) " + _preparedQuery.mid(pos);

    pos = query.indexOf("ORDER BY", Qt::CaseInsensitive);
    if (pos != -1)
        query.remove(pos, query.length());

    QSqlQuery q {drv->createResult()};

    if (!q.prepare(query))
    {
        log_error_m << "Size of result unavailable"
                    << ". Detail: Failed prepare Sql-statement";
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
        log_error_m << "Size of result unavailable"
                    << ". Detail: Failed execute Sql-statement";
        return -1;
    }

    q.first();
    return q.record().value(0).toInt();
    return 0;
}

int Result::numRowsAffected()
{
    SQLLEN affectedRowCount = 0;
    SQLRETURN r = SQLRowCount(_stmt, &affectedRowCount);
    if (r == SQL_SUCCESS)
        return affectedRowCount;
    else
        PRINT_ERROR("QODBCResult::numRowsAffected: Unable to count affected rows", this->_drv);

    return -1;
}

QSqlRecord Result::record() const
{
    if (!isActive() || !isSelect())
        return QSqlRecord();

    return _recInfo;
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
    if (connOpts.contains("SQL_ATTR_ODBC_VERSION=SQL_OV_ODBC3", Qt::CaseInsensitive))
        return SQL_OV_ODBC3;
    return SQL_OV_ODBC2;
}

bool Driver::setConnectionOptions(const QString& connOpts)
{
    // Set any connection attributes
    const QStringList opts(connOpts.split(QLatin1Char(';'), QString::SkipEmptyParts));
    SQLRETURN r = SQL_SUCCESS;
    for (int i = 0; i < opts.count(); ++i)
    {
        const QString tmp(opts.at(i));
        int idx;
        if ((idx = tmp.indexOf(QLatin1Char('='))) == -1)
        {
            log_warn_m <<"QODBCDriver::open: Illegal connect option value '" << tmp << '\'';
            continue;
        }
        const QString opt(tmp.left(idx));
        const QString val(tmp.mid(idx + 1).simplified());
        SQLUINTEGER v = 0;

        r = SQL_SUCCESS;
        if (opt.toUpper() == "SQL_ATTR_ACCESS_MODE")
        {
            if (val.toUpper() == "SQL_MODE_READ_ONLY")
            {
                v = SQL_MODE_READ_ONLY;
            }
            else if (val.toUpper() == "SQL_MODE_READ_WRITE")
            {
                v = SQL_MODE_READ_WRITE;
            }
            else
            {
                log_warn_m << "QODBCDriver::open: Unknown option value '" << val << '\'';
                continue;
            }
            r = SQLSetConnectAttr(_dbc, SQL_ATTR_ACCESS_MODE, (SQLPOINTER) size_t(v), 0);
        }
        else if (opt.toUpper() == "SQL_ATTR_CONNECTION_TIMEOUT")
        {
            v = val.toUInt();
            r = SQLSetConnectAttr(_dbc, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER) size_t(v), 0);
        }
        else if (opt.toUpper() == "SQL_ATTR_LOGIN_TIMEOUT")
        {
            v = val.toUInt();
            r = SQLSetConnectAttr(_dbc, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER) size_t(v), 0);
        }
        else if (opt.toUpper() == "SQL_ATTR_CURRENT_CATALOG")
        {
            val.utf16(); // 0 terminate
            r = SQLSetConnectAttr(_dbc, SQL_ATTR_CURRENT_CATALOG,
                                    detail::toSQLTCHAR(val).data(),
                                    val.length()*sizeof(ushort));
        }
        else if (opt.toUpper() == "SQL_ATTR_METADATA_ID")
        {
            if (val.toUpper() == "SQL_TRUE")
            {
                v = SQL_TRUE;
            }
            else if (val.toUpper() == "SQL_FALSE")
            {
                v = SQL_FALSE;
            }
            else
            {
                log_warn_m << "QODBCDriver::open: Unknown option value '" << val << '\'';
                continue;
            }
            r = SQLSetConnectAttr(_dbc, SQL_ATTR_METADATA_ID, (SQLPOINTER) size_t(v), 0);
        }
        else if (opt.toUpper() == "SQL_ATTR_PACKET_SIZE")
        {
            v = val.toUInt();
            r = SQLSetConnectAttr(_dbc, SQL_ATTR_PACKET_SIZE, (SQLPOINTER) size_t(v), 0);
        }
        else if (opt.toUpper() == "SQL_ATTR_TRACEFILE")
        {
            val.utf16(); // 0 terminate
            r = SQLSetConnectAttr(_dbc, SQL_ATTR_TRACEFILE,
                                    detail::toSQLTCHAR(val).data(),
                                    val.length()*sizeof(ushort));
        }
        else if (opt.toUpper() == "SQL_ATTR_TRACE")
        {
            if (val.toUpper() == "SQL_OPT_TRACE_OFF")
            {
                v = SQL_OPT_TRACE_OFF;
            }
            else if (val.toUpper() == "SQL_OPT_TRACE_ON")
            {
                v = SQL_OPT_TRACE_ON;
            } else {
                log_warn_m << "QODBCDriver::open: Unknown option value '" << val << '\'';
                continue;
            }
            r = SQLSetConnectAttr(_dbc, SQL_ATTR_TRACE, (SQLPOINTER) size_t(v), 0);
        }
        else if (opt.toUpper() == "SQL_ATTR_CONNECTION_POOLING")
        {
            if (val == "SQL_CP_OFF")
                v = SQL_CP_OFF;
            else if (val.toUpper() == "SQL_CP_ONE_PER_DRIVER")
                v = SQL_CP_ONE_PER_DRIVER;
            else if (val.toUpper() == "SQL_CP_ONE_PER_HENV")
                v = SQL_CP_ONE_PER_HENV;
            else if (val.toUpper() == "SQL_CP_DEFAULT")
                v = SQL_CP_DEFAULT;
            else
            {
                log_warn_m << "QODBCDriver::open: Unknown option value '" << val << '\'';
                continue;
            }
            r = SQLSetConnectAttr(_dbc, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER) size_t(v), 0);
        }
        else if (opt.toUpper() == "SQL_ATTR_CP_MATCH")
        {
            if (val.toUpper() == "SQL_CP_STRICT_MATCH")
                v = SQL_CP_STRICT_MATCH;
            else if (val.toUpper() == "SQL_CP_RELAXED_MATCH")
                v = SQL_CP_RELAXED_MATCH;
            else if (val.toUpper() == "SQL_CP_MATCH_DEFAULT")
                v = SQL_CP_MATCH_DEFAULT;
            else
            {
                log_warn_m << "QODBCDriver::open: Unknown option value '" << val << '\'';
                continue;
            }
            r = SQLSetConnectAttr(_dbc, SQL_ATTR_CP_MATCH, (SQLPOINTER) size_t(v), 0);
        }
        else if (opt.toUpper() == "SQL_ATTR_ODBC_VERSION")
        {
            // Already handled in QODBCDriver::open()
            continue;
        }
        else
        {
                log_warn_m << "QODBCDriver::open: Unknown connection attribute '" << opt << '\'';
        }
        if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
            PRINT_ERROR(QString("QODBCDriver::open: Unable to set connection attribute'%1'").arg(opt), this);
    }
    return true;
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

    r = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &_env);
    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        PRINT_ERROR("QODBCDriver::open: Unable to allocate environment", this);
        setOpenError(true);
        return false;
    }

    r = SQLSetEnvAttr(_env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_UINTEGER);

    r = SQLAllocHandle(SQL_HANDLE_DBC, _env, &_dbc);

    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        PRINT_ERROR("QODBCDriver::open: Unable to allocate connection", this);
        setOpenError(true);
        //cleanup();
        return false;
    }

    //setConnectionOptions(connOpts);

    // Create the connection string
    QString connQStr;

    connQStr += "Server="+host+","+QString::number(port);
    connQStr += ";Database="+db;

    if (!user.isEmpty())
        connQStr += ";UID=" + user;
    if (!password.isEmpty())
        connQStr += ";PWD=" + password;

    connQStr += ";"+connOpts;

    SQLSMALLINT cb;
    QVarLengthArray<ushort> connOut(1024);
    memset(connOut.data(), 0, connOut.size() * sizeof(ushort));
    r = SQLDriverConnectW(_dbc, NULL, detail::toSQLTCHAR(connQStr).data(), (SQLSMALLINT)connQStr.length(), connOut.data(), 1024, &cb, /*SQL_DRIVER_NOPROMPT*/0);

    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
    {
        setLastError(detail::qMakeError(tr("Unable to connect"), QSqlError::ConnectionError, this));
        setOpenError(true);
        return false;
    }

    setOpen(true);
    setOpenError(false);
    if (dbmsType() == MSSqlServer)
    {
        QSqlQuery i(createResult());
        i.exec("SET QUOTED_IDENTIFIER ON");
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

    SQLRETURN r;

    if (_dbc)
    {
        // Open statements/descriptors handles are automatically cleaned up by SQLDisconnect
        if (isOpen())
        {
            r = SQLDisconnect(_dbc);
            if (r != SQL_SUCCESS)
                PRINT_ERROR("QODBCDriver::disconnect: Unable to disconnect datasource", this);
        }

        r = SQLFreeHandle(SQL_HANDLE_DBC, _dbc);
        if (r != SQL_SUCCESS)
            PRINT_ERROR("QODBCDriver::cleanup: Unable to free connection handle", this);
        _dbc = nullptr;
    }

    if (_env)
    {
        r = SQLFreeHandle(SQL_HANDLE_ENV, _env);
        if (r != SQL_SUCCESS)
            PRINT_ERROR("QODBCDriver::cleanup: Unable to free environment handle", this);
        _env = 0;
    }


    _threadId = 0;
    _transactAddr = 0;

    setOpen(false);
    setOpenError(false);

    log_verbose_m << "Database is closed. Connect: " << addrToNumber(_dbc);
}

bool Driver::isOpen() const
{
    return _dbc;
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
                     << ". Connect: " << addrToNumber(_dbc);
    }
    else
        log_warn_m << "Failed capture transaction address: "  << addrToNumber(transact)
                   << ". Already captured: " << addrToNumber(_transactAddr)
                   << ". Connect: " << addrToNumber(_dbc);
}

void Driver::releaseTransactAddr(Transaction* transact)
{
    if (_transactAddr == transact)
    {
        _transactAddr = 0;
        log_debug2_m << "Transaction address released: " << addrToNumber(transact)
                     << ". Connect: " << addrToNumber(_dbc);
    }
    else
        log_warn_m << "Failed release transaction address: "  << addrToNumber(transact)
                   << ". Already captured: " << addrToNumber(_transactAddr)
                   << ". Connect: " << addrToNumber(_dbc);
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
        res.replace(QLatin1Char('"'), "\"\"");
        res.prepend(QLatin1Char('"')).append(QLatin1Char('"'));
        res.replace(QLatin1Char('.'), "\".\"");
    }
    return res;
}

void Driver::abortOperation(/*const SQLHANDLE stmt*/)
{
    log_verbose_m << "Abort sql-operation"
                  << ". Connect: " << addrToNumber(_dbc)
                  << " (call from thread: " << trd::gettid() << ")";

    _operationIsAborted = true;

    emit abortStatement();
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

int resultSize(const QSqlQuery& q, const DriverPtr& drv)
{
    if (const Result* r = dynamic_cast<const Result*>(q.result()))
        return r->size2(drv);

    return -1;
}

} // namespace mssql
} // namespace db

#undef log_error_m
#undef log_warn_m
#undef log_info_m
#undef log_verbose_m
#undef log_debug_m
#undef log_debug2_m
