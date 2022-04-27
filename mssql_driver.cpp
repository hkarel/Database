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
#include "shared/safe_singleton.h"
#include "shared/logger/logger.h"
#include "shared/logger/format.h"
#include "shared/qt/quuidex.h"
#include "shared/qt/stream_init.h"
#include "shared/qt/logger_operators.h"
#include "shared/thread/thread_utils.h"

#include <QDateTime>
#include <QVariant>
#include <QSqlField>
#include <QSqlIndex>
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

namespace db {
namespace mssql {

namespace {

inline quint64 addrToNumber(void* addr)
{
    return reinterpret_cast<QIntegerForSizeof<void*>::Unsigned>(addr);
}

const SQLINTEGER  COL_NAME_SIZE = 256;
const SQLSMALLINT TABLE_NAME_SIZE = 128;
const SQLINTEGER  DATETIME_PRECISION = 19;

const SQLSMALLINT qParamType[4] =
{
    SQL_PARAM_INPUT,
    SQL_PARAM_INPUT,
    SQL_PARAM_OUTPUT,
    SQL_PARAM_INPUT_OUTPUT
};

QPair<QString, int> warnODBCHandle(int handleType, SQLHANDLE handle)
{
    int index = 1;
    QString descript;
    int errorCode = 0;

    while (true)
    {
        SQLINTEGER errCode = 0; // Нативный код ошибки, возвращаемый функцией SQLGetDiagRecW
        SQLSMALLINT descrLen = 0;
        ushort state[SQL_SQLSTATE_SIZE + 1];

        SQLRETURN rc;
        rc = SQLGetDiagRecW(handleType, handle, index, state, &errCode,
                            0, 0, &descrLen);

        if (!SQL_SUCCEEDED(rc) || (descrLen <= 0))
        {
            //descript.resize(msgLen + 1);
            log_error_m << "Failed get message length";
            return {descript, errorCode};
        }

        ushort descr[descrLen + 2] = {0};
        rc = SQLGetDiagRecW(handleType, handle, index, state, &errCode,
                            (SQLWCHAR*)descr, descrLen + 1, 0);

        if (SQL_SUCCEEDED(rc))
        {
            errorCode = errCode;
            QString tmpstore = QString::fromUtf16(descr, descrLen);
            if (descript != tmpstore)
            {
                if (!descript.isEmpty())
                    descript += QChar(' ');
                descript += tmpstore;
            }
        }
        else if (rc == SQL_ERROR
                 || rc == SQL_INVALID_HANDLE
                 || rc == SQL_NO_DATA)
        {
            return {descript, errorCode};
        }
        ++index;
    }

    return {descript, errorCode};
}

//QString qODBCWarn(const SQLHANDLE stmt, const SQLHANDLE envHandle = 0,
//                  const SQLHANDLE dbc = 0, int* nativeCode = 0)
//{
//    QString result;

//    if (stmt)
//    {
//        const QString hMessage = qWarnODBCHandle(SQL_HANDLE_STMT, stmt, nativeCode);
//        if (!hMessage.isEmpty())
//        {
//            if (!result.isEmpty())
//                result += QChar(' ');
//            result += hMessage;
//        }
//    }

//    if (envHandle)
//        result += qWarnODBCHandle(SQL_HANDLE_ENV, envHandle, nativeCode);

//    if (dbc)
//    {
//        QString dMessage = qWarnODBCHandle(SQL_HANDLE_DBC, dbc, nativeCode);
//        if (!dMessage.isEmpty())
//        {
//            if (!result.isEmpty())
//                result += QChar(' ');
//            result += dMessage;
//        }
//    }

//    return result;
//}

QVariant::Type decodeODBCType(SQLSMALLINT sqlType, bool isSigned = true)
{
    switch (sqlType)
    {
        case SQL_FLOAT:      // [float]
            return QVariant::Type(qMetaTypeId<float>());

        case SQL_DECIMAL:    // [decimal](18, 0)
        case SQL_NUMERIC:    // [numeric](18, 0)
        case SQL_REAL:       // [real]
            return QVariant::Double;

        case SQL_SMALLINT:   // [smallint]
            return (isSigned) ? QVariant::Int : QVariant::UInt;

        case SQL_INTEGER:    // [int]
            return (isSigned) ? QVariant::Int : QVariant::UInt;

        case SQL_BIT:        // [bit]
            return QVariant::Bool;

        case SQL_TINYINT:    // [tinyint]
            return QVariant::Char;

        case SQL_BIGINT:     // [bigint]
            return (isSigned) ? QVariant::LongLong : QVariant::ULongLong;

        case SQL_BINARY:     // [binary](n), [timestamp]
        case SQL_VARBINARY:  // [varbinary](n), [varbinary](max)
            return QVariant::ByteArray;

        case SQL_TYPE_DATE:  // [date]
            return QVariant::Date;

        case SQL_SS_TIME2:   // [time](7)
            return QVariant::Time;

        case SQL_TYPE_TIMESTAMP: // [datetime], [datetime2](7), [smalldatetime]
        case SQL_SS_TIMESTAMPOFFSET: // [datetimeoffset](7)
            return QVariant::DateTime;

        case SQL_WCHAR:      // [nchar](10)
        case SQL_WVARCHAR:   // [nvarchar](50), [nvarchar](max)
        case SQL_CHAR:       // [char](n)
        case SQL_VARCHAR:    // [varchar](n),
            return QVariant::String;

#if (ODBCVER >= 0x0350)
        case SQL_GUID:
            return QVariant::Uuid;
#endif
    }

    log_warn_m << "decodeODBCType(): unknown datatype: " << int(sqlType);
    return QVariant::Invalid;
}

QString buffToString(const char* buff)
{
    return QString::fromUtf8(buff);
}

QString buffToString(const ushort* buff)
{
    return QString::fromUtf16(buff);
}

template<typename CharType>
QString stringData(SQLHANDLE stmt, int column, int colSize)
{
    if (colSize <= 0)     colSize = 256;
    if (colSize >  65536) colSize = 65536;

    ++colSize; // make sure there is room for more than the 0 termination

    QString fieldVal;
    SQLLEN lenIndicator = 0;
    CharType buff[colSize] = {0};

    while (true)
    {
        SQLRETURN rc = SQLGetData(stmt, column + 1, SQL_C_TCHAR, buff,
                                  colSize * sizeof(buff), &lenIndicator);

        if (SQL_SUCCEEDED(rc))
        {
            if (lenIndicator == SQL_NULL_DATA)
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
            if (lenIndicator == SQL_NO_TOTAL)
            {
                fieldVal += buffToString(buff);
                continue;
            }
            // if SQL_SUCCESS_WITH_INFO is returned, indicating that
            // more data can be fetched, the length indicator does NOT
            // contain the number of bytes returned - it contains the
            // total number of bytes that CAN be fetched
//            int rSize = (rc == SQL_SUCCESS_WITH_INFO)
//                        ? colSize
//                        : int(lengthIndicator / sizeof(ushort));

            fieldVal += buffToString(buff);

            if (lenIndicator < SQLLEN(colSize * sizeof(CharType)))
            {
                // workaround for Drivermanagers that don't return SQL_NO_DATA
                break;
            }
        }
        else if (rc == SQL_NO_DATA)
        {
            break;
        }
        else
        {
            auto [detail, code] = warnODBCHandle(SQL_HANDLE_STMT, stmt);
            log_error_m << log_format(
                "Failed fetching data. Detail: %?. Error code: %?", detail, code);

            fieldVal.clear();
            break;
        }
    }

    return fieldVal;
}

inline QString stringDataA(SQLHANDLE stmt, int column, int colSize)
{
    return stringData<char>(stmt, column, colSize);
}

inline QString stringDataW(SQLHANDLE stmt, int column, int colSize)
{
    return stringData<ushort>(stmt, column, colSize);
}

QVariant binaryData(SQLHANDLE stmt, int column)
{
    SQLSMALLINT colNameLen;
    SQLSMALLINT colType;
    SQLULEN colSize;
    SQLSMALLINT colScale;
    SQLSMALLINT nullable;
    SQLLEN lengthIndicator = 0;
    ushort colName[COL_NAME_SIZE] = {0};

    SQLRETURN rc = SQLDescribeColW(stmt, column + 1, colName, COL_NAME_SIZE,
                                   &colNameLen, &colType, &colSize, &colScale, &nullable);
    if (!SQL_SUCCEEDED(rc))
    {
        log_warn_m << "Unable get describe for column " << column;
        return QVariant::Invalid;
    }

    // SQLDescribeCol may return 0 if size cannot be determined
    if (colSize == 0)     colSize = 255;
    if (colSize >  65536) colSize = 65536;

    QByteArray fieldVal;
    fieldVal.resize(colSize);

    ulong read = 0;
    while (true)
    {
        // Чтение блока 64КБ
        rc = SQLGetData(stmt, column + 1, SQL_C_BINARY, (char*)(fieldVal.constData() + read),
                        colSize, &lengthIndicator);

        if (!SQL_SUCCEEDED(rc))
        {
            log_error_m << "Failed get data chunk. Column: " << column;
            return QVariant::Invalid;
        }

        if (lengthIndicator == SQL_NULL_DATA)
            return QVariant(QVariant::ByteArray);

        // Если есть ещё данные, то будет продолжено чтение следующих 64КБ
        if (lengthIndicator > SQLLEN(colSize) || lengthIndicator == SQL_NO_TOTAL)
        {
            read += colSize;
            colSize = 65536;
        }
        else
        {
            read += lengthIndicator;
        }
        if (rc == SQL_SUCCESS)
        { // the whole field was read in one chunk
            fieldVal.resize(read);
            break;
        }
        // Увеличение буфера на размер прочитанного блока 64КБ
        fieldVal.resize(fieldVal.size() + colSize);
    }

    return fieldVal;
}

QVariant guidData(SQLHANDLE stmt, int column)
{
    SQLSMALLINT colNameLen;
    SQLSMALLINT colType;
    SQLULEN colSize;
    SQLSMALLINT colScale;
    SQLSMALLINT nullable;
    SQLLEN lenIndicator = 0;
    SQLRETURN rc = SQL_ERROR;
    ushort colName[COL_NAME_SIZE] = {0};

    rc = SQLDescribeColW(stmt, column + 1, colName, COL_NAME_SIZE,
                         &colNameLen, &colType, &colSize, &colScale, &nullable);
    if (!SQL_SUCCEEDED(rc))
    {
        log_warn_m << "Unable get describe for column " << column;
        return QVariant::Invalid;
    }

    // SQLDescribeCol may return 0 if size cannot be determined
    if (!colSize)
        return QVariant(QVariant::Uuid);

    if (colSize != 36)
    {
        log_warn_m << log_format("Guid incorrect size %? (expect %?) "
                                 "for column %?", colSize, 36, column);
        return QVariant::Invalid;
    }

    // api = 36, но после 16 - мусор
    colSize = 16;

    QByteArray fieldVal;
    fieldVal.resize(colSize);

    rc = SQLGetData(stmt, column + 1, SQL_C_GUID, (char*)fieldVal.constData(),
                    colSize, &lenIndicator);
    if (!SQL_SUCCEEDED(rc))
        return QVariant(QVariant::Uuid);

    if (lenIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::Uuid);

    QUuid uuid;
    memcpy((char*)&uuid, fieldVal.data(), colSize);

    return QVariant(uuid);
}

QVariant intData(SQLHANDLE stmt, int column, bool isSigned = true)
{
    SQLINTEGER intbuf = 0;
    SQLLEN lenIndicator = 0;
    SQLRETURN rc = SQLGetData(stmt, column + 1, (isSigned) ? SQL_C_SLONG : SQL_C_ULONG,
                              &intbuf, sizeof(intbuf), &lenIndicator);
    if (!SQL_SUCCEEDED(rc))
        return QVariant(QVariant::Invalid);

    if (lenIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::Int);

    if (isSigned)
        return int(intbuf);
    else
        return uint(intbuf);
}

QVariant doubleData(SQLHANDLE stmt, int column)
{
    SQLDOUBLE dblbuf;
    SQLLEN lengthIndicator = 0;
    SQLRETURN rc = SQLGetData(stmt, column + 1, SQL_C_DOUBLE,
                              &dblbuf, sizeof(dblbuf), &lengthIndicator);
    if (!SQL_SUCCEEDED(rc))
        return QVariant(QVariant::Invalid);

    if (lengthIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::Double);

    return double(dblbuf);
}

long strToHexToVal(SQL_NUMERIC_STRUCT& numStruct)
{
    int last = 1;
    long value = 0;
    for (int i = 0; i <= 15; ++i)
    {
        int current = (int)numStruct.val[i];
        int a = current % 16;
        int b = current / 16;

        value += last * a;
        last = last * 16;
        value += last * b;
        last = last * 16;
    }
    return value;
}

QVariant numericData(SQLHANDLE stmt, int column)
{
    SQLLEN lenIndicator = 0;
    SQL_NUMERIC_STRUCT numStruct;
    SQLRETURN rc = SQLGetData(stmt, column + 1, SQL_C_NUMERIC,
                              &numStruct, sizeof(numStruct), &lenIndicator);
    long divisor = 1;
    if (numStruct.scale > 0)
    {
        for (int i = 0; i < numStruct.scale; ++i)
            divisor = divisor * 10;
    }

    long myvalue = strToHexToVal(numStruct);
    double finalVal = double(myvalue) / divisor;

    int sign = 1;
    if (!numStruct.sign)
        sign = -1;

    finalVal *= sign;

    if (!SQL_SUCCEEDED(rc))
        return QVariant(QVariant::Invalid);

    if (lenIndicator == SQL_NULL_DATA)
        //return QVariant::Type(qMetaTypeId<double>());
        return QVariant::Double;

    return finalVal;
}

QVariant bigIntData(SQLHANDLE stmt, int column, bool isSigned = true)
{
    SQLBIGINT lngbuf = 0;
    SQLLEN lenIndicator = 0;
    SQLRETURN rc = SQLGetData(stmt, column + 1, (isSigned) ? SQL_C_SBIGINT : SQL_C_UBIGINT,
                              &lngbuf, sizeof(lngbuf), &lenIndicator);
    if (!SQL_SUCCEEDED(rc))
        return QVariant(QVariant::Invalid);

    if (lenIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::LongLong);

    if (isSigned)
        return qint64(lngbuf);
    else
        return quint64(lngbuf);
}

QVariant bitData(SQLHANDLE stmt, int column)
{
    bool boolbuf = 0;
    SQLLEN lenIndicator = 0;
    SQLRETURN rc = SQLGetData(stmt, column + 1, SQL_C_BIT,
                              &boolbuf, sizeof(boolbuf), &lenIndicator);
    if (!SQL_SUCCEEDED(rc))
        return QVariant(QVariant::Invalid);

    if (lenIndicator == SQL_NULL_DATA)
        return QVariant(QVariant::Bool);

    return boolbuf;
}

//bool isAutoValue(const SQLHANDLE stmt, int column)
//{
//    SQLLEN nNumericAttribute = 0; // Check for auto-increment
//    const SQLRETURN rc = SQLColAttribute(stmt, column + 1, SQL_DESC_AUTO_UNIQUE_VALUE,
//                                         0, 0, 0, &nNumericAttribute);
//    if (!SQL_SUCCEEDED(rc))
//    {
//        auto [detail, code] = warnODBCHandle(SQL_HANDLE_STMT, stmt);
//        log_error_m << log_format("Unable to get autovalue attribute for column %?"
//                                  ". Detail: %?. Error code: %?", column, detail, code);

//        //PRINT_ERROR("qMakeField: Unable to get autovalue attribute for column " + QString::number(column), stmt);
//        return false;
//    }
//    return (nNumericAttribute != SQL_FALSE);
//}

//// creates a QSqlField from a valid stmt generated
//// by SQLColumns. The stmt has to point to a valid position.
//QSqlField qMakeFieldInfo(const SQLHANDLE stmt, const DriverPtr& driver)
//{
//    QString fname = qGetStringData(stmt, 3, -1);
//    int type = qGetIntData(stmt, 4).toInt(); // column type
//    QSqlField f(fname, qDecodeODBCType(type, driver));
//    QVariant var = qGetIntData(stmt, 6);
//    f.setLength(var.isNull() ? -1 : var.toInt()); // column size
//    var = qGetIntData(stmt, 8).toInt();
//    f.setPrecision(var.isNull() ? -1 : var.toInt()); // precision
//    f.setSqlType(type);
//    int required = qGetIntData(stmt, 10).toInt(); // nullable-flag
//    // required can be SQL_NO_NULLS, SQL_NULLABLE or SQL_NULLABLE_UNKNOWN
//    if (required == SQL_NO_NULLS)
//        f.setRequired(true);
//    else if (required == SQL_NULLABLE)
//        f.setRequired(false);
//    // else we don't know
//    return f;
//}

//QSqlField qMakeFieldInfo(const Result* result, int i)
//{
//    QString errorMessage;

//    QSqlField field = qMakeFieldInfo(result->_stmt, i, &errorMessage);

//    if (!errorMessage.isEmpty())
//        PRINT_ERROR(errorMessage, result);

//    return field;
//}

//QSqlField qMakeFieldInfo(const SQLHANDLE stmt, int i, QString* errorMessage)
//{
//    SQLSMALLINT colNameLen;
//    SQLSMALLINT colType;
//    SQLULEN colSize;
//    SQLSMALLINT colScale;
//    SQLSMALLINT nullable;
//    SQLRETURN rc = SQL_ERROR;

//    //QVarLengthArray<ushort> colName(COLNAMESIZE);
//    ushort colName[COL_NAME_SIZE] = {0};

//    errorMessage->clear();
//    rc = SQLDescribeColW(stmt, i + 1, colName, (SQLSMALLINT)COL_NAME_SIZE,
//                         &colNameLen, &colType, &colSize, &colScale, &nullable);
//    if (!SQL_SUCCEEDED(rc))
//    {
//        *errorMessage = QStringLiteral("qMakeField: Unable to describe column ") + QString::number(i);
//        return QSqlField();
//    }

//    SQLLEN unsignedFlag = SQL_FALSE;
//    rc = SQLColAttribute (stmt, i + 1, SQL_DESC_UNSIGNED, 0, 0, 0, &unsignedFlag);
//    if (!SQL_SUCCEEDED(rc))
//    {
//        PRINT_ERROR("qMakeFieldInfo: Unable to get column attributes for column " + QString::number(i), stmt);
//    }

//    QString qColName = QString::fromUtf16(colName);

//    // nullable can be SQL_NO_NULLS, SQL_NULLABLE or SQL_NULLABLE_UNKNOWN
//    QVariant::Type type = qDecodeODBCType(colType, unsignedFlag == SQL_FALSE);
//    QSqlField f(qColName, type);
//    f.setSqlType(colType);
//    f.setLength(colSize == 0 ? -1 : int(colSize));
//    f.setPrecision(colScale == 0 ? -1 : int(colScale));
//    if (nullable == SQL_NO_NULLS)
//        f.setRequired(true);
//    else if (nullable == SQL_NULLABLE)
//        f.setRequired(false);
//    // else we don't know
//    f.setAutoValue(isAutoValue(stmt, i));

//    //QVarLengthArray<ushort> tableName(TABLENAMESIZE);
//    ushort tableName[TABLE_NAME_SIZE] = {0};
//    SQLSMALLINT tableNameLen;
//    rc = SQLColAttribute(stmt, i + 1, SQL_DESC_BASE_TABLE_NAME,
//                         tableName, TABLE_NAME_SIZE, &tableNameLen, 0);
//    if (SQL_SUCCEEDED(rc))
//        f.setTableName(QString::fromUtf16(tableName, tableNameLen));

//    return f;
//}

struct QueryParams
{
    int nparams = {0};
    char**  paramValues  = {0};
    //int*  paramLengths = {0};
    //int*  paramFormats = {0};
    SQLLEN* indicators   = {0};

    QueryParams() = default;
    DISABLE_DEFAULT_COPY(QueryParams)

    ~QueryParams()
    {
        for (int i = 0; i < nparams; ++i)
            free(paramValues[i]);

        delete [] paramValues;
        delete [] indicators;
        //delete [] paramLengths;
        //delete [] paramFormats;
    }

    void init(int nparams)
    {
        this->nparams = nparams;
        paramValues = new char* [nparams];
        indicators  = new SQLLEN[nparams];
        //paramLengths = new int   [nparams];
        //paramFormats = new int   [nparams];
        for (int i = 0; i < nparams; ++i)
        {
            paramValues[i] = 0;
            indicators [i] = 0;
            //paramLengths[i] = 0;
            //paramFormats[i] = 1;
        }
    }
};

} // namespace

//------------------------------- Transaction --------------------------------

Transaction::Transaction(const DriverPtr& drv) : _drv(drv)
{
    log_debug2_m << "Transaction ctor. Address: " << addrToNumber(this);
    Q_ASSERT(_drv.get());
    _drv->captureTransactAddr(this);
}

Transaction::~Transaction()
{
    log_debug2_m << "Transaction dtor. Address: " << addrToNumber(this);
    if (isActive())
        rollback();
    _drv->releaseTransactAddr(this);
}

bool Transaction::begin(IsolationLevel /*isolationLevel*/)
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
        log_error_m << log_format("Transaction already begun: %?/%?",
                                  addrToNumber(_drv->_connect), _transactId);
        return false;
    }

    #define CHECK_ERROR_(MSG) \
        if (!SQL_SUCCEEDED(rc)) { \
            auto [detail, code] = warnODBCHandle(SQL_HANDLE_STMT, stmt); \
            log_error_m << MSG \
                        << ". Detail: " << detail \
                        << ". Error code: " << code; \
            return false; \
        }

//    SQLHANDLE stmt;
//    SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, _drv->_connect, &stmt);
//    CHECK_ERROR_("Failed to allocate statement handle")

//    rc = SQLExecDirect(stmt, (SQLCHAR*) "SELECT CURRENT_TRANSACTION_ID() as tid;", SQL_NTS);
//    CHECK_ERROR_("Failed execute CURRENT_TRANSACTION_ID()")

//    SQLLEN transactId = 0, szTransactId = 0;
//    rc = SQLBindCol(stmt, 1, SQL_C_SLONG, &transactId, sizeof(transactId), &szTransactId);
//    CHECK_ERROR_("Failed bind transaction id variable")

//    rc = SQLFetch(stmt);
//    CHECK_ERROR_("Failed fetch transaction id variable")

//    //rc = SQLExecDirect(stmt, (SQLCHAR*) "SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED", SQL_NTS);
//    rc = SQLCloseCursor(stmt);
//    CHECK_ERROR_("Failed close cursor");

//    rc = SQLFreeStmt(stmt, SQL_RESET_PARAMS);
//    CHECK_ERROR_("Failed free the statement handle")

    #undef CHECK_ERROR_

    _transactId = 0;/*transactId;*/
    _isActive = true;

    log_debug2_m << log_format("Transaction begin: %?/%?",
                               addrToNumber(_drv->_connect), _transactId);
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

    SQLRETURN rc = SQLEndTran(SQL_HANDLE_DBC, _drv->_connect, SQL_COMMIT);
    if (!SQL_SUCCEEDED(rc))
    {
        auto [detail, code] = warnODBCHandle(SQL_HANDLE_DBC, _drv->_connect);
        log_error_m << log_format(
            "Failed commit transaction: %?/%?. Detail: %?. Error code: %?",
            addrToNumber(_drv->_connect), _transactId, detail, code);
        _isActive = false;
        _transactId = -1;
        return false;
    }

    log_debug2_m << log_format("Transaction commit: %?/%?",
                               addrToNumber(_drv->_connect), _transactId);
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

    SQLRETURN rc = SQLEndTran(SQL_HANDLE_DBC, _drv->_connect, SQL_ROLLBACK);
    if (!SQL_SUCCEEDED(rc))
    {
        auto [detail, code] = warnODBCHandle(SQL_HANDLE_DBC, _drv->_connect);
        log_error_m << log_format(
            "Failed rollback transaction: %?/%?. Detail: %?. Error code: %?",
            addrToNumber(_drv->_connect), _transactId, detail, code);

        _isActive = false;
        _transactId = -1;
        return false;
    }

    log_debug2_m << log_format("Transaction rollback: %?/%?",
                               addrToNumber(_drv->_connect), _transactId);
    _isActive = false;
    _transactId = -1;
    return true;
}

bool Transaction::isActive() const
{
    return _isActive;
}

//---------------------------------- Result ----------------------------------

// Выводит в лог сокращенное описание ошибки без идентификатора транзакции
#define SET_LAST_ERROR1(MSG, ERR_TYPE) \
    setLastError1(MSG, ERR_TYPE, __func__, __LINE__);

// Выводит в лог детализированное описание ошибки с идентификатором транзакции
#define SET_LAST_ERROR2(MSG, ERR_TYPE, DETAIL, CODE) \
    setLastError2(MSG, ERR_TYPE, __func__, __LINE__, DETAIL, CODE);

#define CHECK_ERROR(MSG, ERR_TYPE) \
    checkError(MSG, ERR_TYPE, rc, __func__, __LINE__)

Result::Result(const DriverPtr& drv, ForwardOnly forwardOnly)
    : SqlCachedResult(drv.get()),
      _drv(drv)
{
    Q_ASSERT(_drv.get());
    setForwardOnly(forwardOnly == ForwardOnly::Yes);

    chk_connect_d(_drv.get(), &Driver::abortStatement, this, &Result::abortStatement);
}

Result::Result(const Transaction::Ptr& trans, ForwardOnly forwardOnly)
    : SqlCachedResult(trans->_drv.get()),
      _drv(trans->_drv),
      _externalTransact(trans)
{
    Q_ASSERT(_drv.get());
    setForwardOnly(forwardOnly == ForwardOnly::Yes);

    chk_connect_d(_drv.get(), &Driver::abortStatement, this, &Result::abortStatement);
}

Result::~Result()
{
    cleanup();
}

void Result::setLastError1(const QString& msg, QSqlError::ErrorType type,
                           const char* func, int line)
{
    setLastError(QSqlError("MssqlResult", msg, type, "1"));
    constexpr const char* file_name = alog::detail::file_name(__FILE__);
    alog::logger().error(file_name, func, line, "MssqlDrv") << msg;
}

void Result::setLastError2(const QString& msg, QSqlError::ErrorType type,
                           const char* func, int line, const QString& detail,
                           int code)
{
    setLastError(QSqlError("MssqlResult", msg, type, "1"));
    constexpr const char* file_name = alog::detail::file_name(__FILE__);
    quint64 connectId = addrToNumber(_drv->_connect);
    alog::Line logLine = alog::logger().error(file_name, func, line, "MssqlDrv")
        << log_format("%?. Transact: %?/%?", msg, connectId, transactId());
    if (!detail.isEmpty())
        logLine << log_format(". Detail: %?. Error code: %?", detail , code);
}

bool Result::checkError(const char* msg, QSqlError::ErrorType type, SQLRETURN rc,
                        const char* func, int line)
{
    if (!SQL_SUCCEEDED(rc))
    {
        auto [detail, code] = warnODBCHandle(SQL_HANDLE_STMT, _stmt);
        setLastError2(msg, type, func, line, detail, code);
        return true;
    }
    return false;
}

bool Result::isSelectSql() const
{
    return isSelect();
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

    if (_stmt)
    {
        SQLRETURN rc = SQLFreeStmt(_stmt, SQL_RESET_PARAMS);
        CHECK_ERROR("Failed free the statement handle", QSqlError::StatementError);
        _stmt = nullptr;
    }

    _preparedQuery.clear();
    _numRowsAffected = -1;
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
        SET_LAST_ERROR1("Failed begin internal transaction",
                        QSqlError::TransactionError)
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
        SET_LAST_ERROR1("Failed commit internal transaction",
                        QSqlError::TransactionError)
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
        SET_LAST_ERROR1("Failed rollback internal transaction",
                        QSqlError::TransactionError)
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
        SET_LAST_ERROR1("Sql-operation aborted", QSqlError::UnknownError)
        return false;
    }
    if (!_drv->isOpen() || _drv->isOpenError())
    {
        SET_LAST_ERROR1("Database not open", QSqlError::ConnectionError)
        return false;
    }

    cleanup();
    setActive(false);
    setAt(QSql::BeforeFirstRow);

    if (!beginInternalTransact())
        return false;

    if (alog::logger().level() == alog::Level::Debug2)
    {
        QString sql = query;
        static QRegExp reg {R"(\s{2,})"};
        sql.replace(reg, " ");
        sql.replace("[ ", "[");
        sql.replace(" ]", "]");
        sql.replace(" ,", ",");
        if (!sql.isEmpty() && (sql[0] == QChar(' ')))
            sql.remove(0, 1);
        log_debug2_m << log_format("Begin prepare query. Transact: %?/%?. %?",
                                   addrToNumber(_drv->_connect), transactId(), sql);
    }

    SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_STMT, _drv->_connect, &_stmt);
    if (CHECK_ERROR("Failed to allocate statement handle",
                    QSqlError::StatementError))
        return false;

    rc = SQLSetStmtAttr(_stmt, SQL_ATTR_CURSOR_TYPE,
                            (SQLPOINTER)SQL_CURSOR_FORWARD_ONLY, SQL_IS_UINTEGER);

    if (CHECK_ERROR("Failed set 'SQL_CURSOR_FORWARD_ONLY' attribute",
                    QSqlError::StatementError))
        return false;

    rc = SQLPrepareW(_stmt, (SQLWCHAR*)query.utf16(), query.length());
    if (CHECK_ERROR("Could not prepare statement", QSqlError::StatementError))
        return false;

    // nfields - число столбцов (полей) в каждой строке полученной выборки.
    // При выполнении INSERT или UPDATE запросов, количество столбцов будет
    // равно 0. В этом случае запрос будет установлен как "Not Select"
    SQLSMALLINT nfields;
    rc = SQLNumResultCols(_stmt, &nfields);
    if (CHECK_ERROR("Failed get fields count", QSqlError::StatementError))
        return false;

    setSelect(nfields != 0);
    _preparedQuery = query;

    log_debug2_m << log_format("End prepare query. Transact: %?/%?",
                               addrToNumber(_drv->_connect), transactId());
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
        SET_LAST_ERROR1("Sql-operation aborted", QSqlError::UnknownError)
        return false;
    }
    if (!_drv->isOpen() || _drv->isOpenError())
    {
        SET_LAST_ERROR1("Database not open", QSqlError::ConnectionError)
        return false;
    }

    if (!beginInternalTransact())
        return false;

    log_debug2_m << log_format("Start exec query. Transact: %?/%?",
                               addrToNumber(_drv->_connect), transactId());

    setActive(false);
    setAt(QSql::BeforeFirstRow);

    SQLRETURN rc;

    if (isSelect())
    {
        // Непонятен смысл вызова SQLCloseCursor в данном конкретном месте
        // Необходимо разобраться и понять зачем этот код в оригинальном драйвере
        // rc = SQLCloseCursor(_stmt);
        // CHECK_ERROR("Failed close cursor", QSqlError::StatementError);
    }

    QueryParams params;
    SQLSMALLINT nparams;

    rc = SQLNumParams(_stmt, &nparams);
    if (CHECK_ERROR("Failed get input params count", QSqlError::StatementError))
    {
        rollbackInternalTransact();
        return false;
    }

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
            QString msg = "Parameter mismatch, expected %1, got %2 parameters";
            msg = msg.arg(nparams).arg(values.count());
            SET_LAST_ERROR2(msg, QSqlError::StatementError, {}, 0)
            rollbackInternalTransact();
            return false;
        }

        // bind parameters - only positional binding allowed
        for (int i = 0; i < nparams; ++i)
        {
            const QVariant& val = values[i];

            // Variant со значением Null всегда являются не валидным, поэтому
            // валидность проверятся только для не Null значений
            if (!val.isNull() && !val.isValid())
            {
                QString msg = "Query param%1 is invalid";
                SET_LAST_ERROR2(msg.arg(i), QSqlError::StatementError, {}, 0)
                rollbackInternalTransact();
                return false;
            }

            // TODO Разобтаться с записью NULL данных
            // else if (val.userType() == qMetaTypeId<QUuidEx>())
            // {
            //     const QUuidEx& uuid = val.value<QUuidEx>();
            //     if (uuid.isNull())
            //         continue;
            // }
            // else if (val.userType() == qMetaTypeId<QUuid>())
            // {
            //     const QUuid& uuid = val.value<QUuid>();
            //     if (uuid.isNull())
            //         continue;
            // }

            SQLSMALLINT dataType, decimalDigits, nullable;
            SQLULEN bytesRemaining;

            rc = SQLDescribeParam(_stmt, i + 1, &dataType, &bytesRemaining,
                                  &decimalDigits, &nullable);
            if (!SQL_SUCCEEDED(rc))
            {
                QString msg = "Failed get describe param%1";
                auto [detail, code] = warnODBCHandle(SQL_HANDLE_STMT, _stmt);
                SET_LAST_ERROR2(msg.arg(i), QSqlError::StatementError, detail, code)
                rollbackInternalTransact();
                return false;
            }

            SQLLEN* ind = params.indicators + i;
            if (val.isNull())
                *ind = SQL_NULL_DATA;

            switch (dataType)
            {

                case SQL_TYPE_DATE: // [date]
                {
                    params.paramValues[i] = (char*)malloc(sizeof(DATE_STRUCT));
                    DATE_STRUCT* ds = (DATE_STRUCT*)params.paramValues[i];
                    QDate d = val.toDate();
                    ds->year  = d.year();
                    ds->month = d.month();
                    ds->day   = d.day();


                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_DATE,
                            SQL_DATE,
                            0,
                            0,
                            params.paramValues[i],
                            0,
                            (*ind == SQL_NULL_DATA) ? ind : nullptr);
                    break;
                }
                case SQL_TYPE_TIME: // [time](7)
                case SQL_SS_TIME2:  // [time](7)
                {
                    params.paramValues[i] = (char*)malloc(sizeof(TIME_STRUCT));
                    TIME_STRUCT* ts = (TIME_STRUCT*)params.paramValues[i];
                    QTime t = val.toTime();
                    ts->hour =   t.hour();
                    ts->minute = t.minute();
                    ts->second = t.second();

                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_TIME,
                            SQL_TIME,
                            0,
                            0,
                            params.paramValues[i],
                            0,
                            (*ind == SQL_NULL_DATA) ? ind : nullptr);
                    break;
                }
                case SQL_TYPE_TIMESTAMP: // [datetime], [datetime2](7), [smalldatetime]
                case SQL_SS_TIMESTAMPOFFSET: // [datetimeoffset](7)
                {
                    params.paramValues[i] = (char*)malloc(sizeof(TIMESTAMP_STRUCT));
                    TIMESTAMP_STRUCT* ts = (TIMESTAMP_STRUCT*)params.paramValues[i];

                    QDateTime dt = val.toDateTime();
                    QDate d = dt.date();
                    QTime t = dt.time();

                    ts->year   = d.year();
                    ts->month  = d.month();
                    ts->day    = d.day();
                    ts->hour   = t.hour();
                    ts->minute = t.minute();
                    ts->second = t.second();
                    // (20 includes a separating period)
                    const int precision = DATETIME_PRECISION - 20;
                    if (precision <= 0)
                    {
                        ts->fraction = 0;
                    }
                    else
                    {
                        ts->fraction = t.msec() * 1000000;

                        // (How many leading digits do we want to keep?  With SQL Server 2005, this should be 3: 123000000)
                        int keep = (int)qPow(10.0, 9 - qMin(9, precision));
                        ts->fraction = (ts->fraction / keep) * keep;
                    }

                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_TIMESTAMP,
                            SQL_TIMESTAMP,
                            DATETIME_PRECISION,
                            //((TIMESTAMP_STRUCT*)ba.constData())->fraction,
                            ts->fraction,
                            params.paramValues[i],
                            0,
                            (*ind == SQL_NULL_DATA) ? ind : nullptr);
                    break;
                }
                case SQL_SMALLINT: // [smallint]
                {
                    qint16 v = val.toInt();
                    params.paramValues[i] = (char*)malloc(sizeof(qint16));
                    *((qint16*)params.paramValues[i]) = v;

                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_USHORT,
                            SQL_SMALLINT,
                            0,
                            0,
                            params.paramValues[i],
                            0,
                            (*ind == SQL_NULL_DATA) ? ind : nullptr);
                    break;
                }
                case SQL_BIT: // [bit]
                {
                        bool v = val.toBool();
                        params.paramValues[i] = (char*)malloc(sizeof(qint8));
                        *((qint8*)params.paramValues[i]) = v;

                        rc = SQLBindParameter(
                                _stmt, i + 1,
                                SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                                SQL_C_BIT,
                                SQL_BIT,
                                0,
                                0,
                                params.paramValues[i],
                                0,
                                (*ind == SQL_NULL_DATA) ? ind : nullptr);
                        break;
                }
                case SQL_INTEGER: // [int]
                {
                    qint32 v = val.toInt();
                    params.paramValues[i] = (char*)malloc(sizeof(qint32));
                    *((qint32*)params.paramValues[i]) = v;

                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_SLONG,
                            SQL_INTEGER,
                            0,
                            0,
                            params.paramValues[i],
                            0,
                            (*ind == SQL_NULL_DATA) ? ind : nullptr);
                    break;
                }
                case SQL_TINYINT: // [tinyint]
                {
                    qint8 v = val.toInt();
                    params.paramValues[i] = (char*)malloc(sizeof(qint8));
                    *((qint8*)params.paramValues[i]) = v;

                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_UTINYINT,
                            SQL_TINYINT,
                            15,
                            0,
                            params.paramValues[i],
                            0,
                            (*ind == SQL_NULL_DATA) ? ind : nullptr);
                    break;
                }

                // TODO Сделать реализацию по потребности
                // case SQL_DECIMAL: // [decimal](18, 0)
                // case SQL_NUMERIC: // [numeric](18, 0)

                case SQL_REAL:    // [real]
                case SQL_FLOAT:   // [float]
                {
                    float v = val.toDouble();
                    params.paramValues[i] = (char*)malloc(sizeof(float));
                    *((float*)params.paramValues[i]) = v;

                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_FLOAT,
                            SQL_REAL,
                            0,
                            0,
                            params.paramValues[i],
                            0,
                            (*ind == SQL_NULL_DATA) ? ind : nullptr);
                    break;
                }
                case SQL_DOUBLE:  // [double]
                {
                    double v = val.toDouble();
                    params.paramValues[i] = (char*)malloc(sizeof(double));
                    *((double*)params.paramValues[i]) = v;

                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_DOUBLE,
                            SQL_DOUBLE,
                            0,
                            0,
                            params.paramValues[i],
                            0,
                            (*ind == SQL_NULL_DATA) ? ind : nullptr);
                    break;
                }
                case SQL_BIGINT: // [bigint]
                {
                    qint64 v = val.toLongLong();
                    params.paramValues[i] = (char*)malloc(sizeof(qint64));
                    *((qint64*)params.paramValues[i]) = v;

                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_UBIGINT,
                            SQL_BIGINT,
                            0,
                            0,
                            params.paramValues[i],
                            0,
                            (*ind == SQL_NULL_DATA) ? ind : nullptr);
                    break;
                }
                case SQL_BINARY:    // [binary](n)
                case SQL_VARBINARY: // [varbinary](n), [varbinary](max)
                case SQL_LONGVARBINARY: // !
                {
                    QByteArray v = val.toByteArray();
                    params.paramValues[i] = (char*)malloc(v.length());
                    memcpy(params.paramValues[i], v.constData(), v.length());

                    if (*ind != SQL_NULL_DATA)
                        *ind = v.length();

                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_BINARY,
                            SQL_LONGVARBINARY,
                            v.length(),
                            0,
                            params.paramValues[i],
                            v.length(), // ??? зачем тут длина
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
                        QString msg = "Query param%1 is not UUID type";
                        SET_LAST_ERROR2(msg.arg(i), QSqlError::StatementError, {}, 0)
                        rollbackInternalTransact();
                        return false;
                    }

                    //qint32(bswap_32(*(qint32*)ba.data()));
                    //qint32(bswap_16(*(qint32*)ba.data()));

                    params.paramValues[i] = (char*)malloc(v.length());
                    memcpy(params.paramValues[i], v.constData(), v.length());

                    if (*ind != SQL_NULL_DATA)
                        *ind = v.length();

                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_GUID,
                            SQL_GUID,
                            v.length(),
                            0,
                            params.paramValues[i],
                            v.length(),
                            ind);
                    break;
                }

                // TODO Непонятен размер поля. Может отрезать?
                // case SQL_BIT: //[bit]
                // {
                //     rc = SQLBindParameter(_stmt,
                //                           i + 1,
                //                           qParamType[bindValueType(i) & QSql::InOut],
                //                           SQL_C_BIT,
                //                           SQL_BIT,
                //                           0,
                //                           0,
                //                           (void*)val.constData(),
                //                           0,
                //                           (*ind == SQL_NULL_DATA) ? ind : nullptr);
                //     break;
                // }

                case SQL_WCHAR:    // [nchar](n)
                case SQL_WVARCHAR: // [nvarchar](n), [nvarchar](max)
                case SQL_CHAR:     // [char](n)
                case SQL_VARCHAR:  // [varchar](n), [varchar](max)
                case SQL_LONGVARCHAR:
                {
                    QString str = val.toString();
                    SQLLEN strSize = 0;

                    if (_drv->_wideChar)
                    {
                        strSize = str.length() * sizeof(ushort);
                        params.paramValues[i] = (char*)malloc(strSize);
                        memcpy(params.paramValues[i], str.utf16(), strSize);
                    }
                    else
                    {
                        strSize = str.length() * sizeof(char);
                        params.paramValues[i] = (char*)malloc(strSize);
                        memcpy(params.paramValues[i], str.toUtf8().constData(), strSize);
                    }

                    if (*ind != SQL_NULL_DATA)
                        *ind = strSize;

//                    if (bindValueType(i) & QSql::Out)
//                    {
//                        rc = SQLBindParameter(
//                                _stmt, i + 1,
//                                SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
//                                SQL_C_TCHAR,
//                                (strSize > 254) ? SQL_WLONGVARCHAR : SQL_WVARCHAR,
//                                0, // god knows... don't change this!
//                                0,
//                                params.paramValues[i],
//                                strSize,
//                                ind);
//                        break;
//                    }

                    rc = SQLBindParameter(
                            _stmt, i + 1,
                            SQL_PARAM_INPUT/*qParamType[bindValueType(i) & QSql::InOut]*/,
                            SQL_C_TCHAR,
                            (strSize > 254) ? SQL_WLONGVARCHAR : SQL_WVARCHAR,
                            strSize,
                            0,
                            params.paramValues[i],
                            strSize,
                            ind);
                    break;
                }
                default:
                {
                    QString msg = "Query param%1, is unknown datatype: %2";
                    msg = msg.arg(i).arg(dataType);
                    SET_LAST_ERROR2(msg, QSqlError::StatementError, {}, 0)
                    rollbackInternalTransact();
                    return false;

                }
            }

            if (CHECK_ERROR("Unable to bind variable", QSqlError::StatementError))
            {
                rollbackInternalTransact();
                return false;
            }
        }
    } // if (nparams != 0)

    rc = SQLExecute(_stmt);
    if (CHECK_ERROR("Could not exec prepared statement", QSqlError::StatementError))
    {
        rollbackInternalTransact();
        return false;
    }

    if (isSelectSql())
    {
        SQLSMALLINT nfields;
        rc = SQLNumResultCols(_stmt, &nfields);
        if (CHECK_ERROR("Failed get fields count", QSqlError::StatementError))
        {
            rollbackInternalTransact();
            return false;
        }
        init(nfields);
    }

    if (_drv->operationIsAborted())
    {
        SET_LAST_ERROR1("Sql-operation aborted", QSqlError::UnknownError)
        rollbackInternalTransact();
        return false;
    }

    quint64 transId = transactId();
    quint64 connectId = addrToNumber(_drv->_connect);

    if (!isSelectSql())
        if (!commitInternalTransact())
        {
            log_debug2_m << log_format("Failed exec query. Transact: %?/%?",
                                       connectId, transId);
            return false;
        }

    _numRowsAffected = 0;
    setActive(true);

    return true;
}

void Result::abortStatement()
{
    SQLRETURN rc = SQLCancel(_stmt);
    CHECK_ERROR("Failed abort statement", QSqlError::StatementError);
}

bool Result::gotoNext(SqlCachedResult::ValueCache& row, int rowIdx)
{
    if (_drv->operationIsAborted())
    {
        setAt(QSql::AfterLastRow);
        return false;
    }

    SQLRETURN rc = SQLFetch(_stmt);
    if (!SQL_SUCCEEDED(rc))
    {
        if (rc != SQL_NO_DATA)
        {
            auto [detail, code] = warnODBCHandle(SQL_HANDLE_STMT, _stmt);
            SET_LAST_ERROR2("Failed fetch record", QSqlError::StatementError,
                            detail, code)
        }
        setAt(QSql::AfterLastRow);
        return false;
    }

    SQLSMALLINT nfields;
    rc = SQLNumResultCols(_stmt, &nfields);
    if (CHECK_ERROR("Failed get fields count", QSqlError::StatementError))
    {
        setAt(QSql::AfterLastRow);
        return false;
    }

    for (int i = 0; i < nfields; ++i)
    {
        int idx = rowIdx + i;

        SQLSMALLINT colNameLen;
        SQLSMALLINT colType;
        SQLULEN colSize;
        SQLSMALLINT colScale;
        SQLSMALLINT nullable;
        SQLLEN lenIndicator = 0;
        SQLRETURN rc = SQL_ERROR;

        ushort colName[COL_NAME_SIZE] = {0};
        rc = SQLDescribeColW(_stmt, i + 1, colName, COL_NAME_SIZE, &colNameLen,
                             &colType, &colSize, &colScale, &nullable);
        if (!SQL_SUCCEEDED(rc))
        {
            QString msg = "Failed get describe for column %1";
            auto [detail, code] = warnODBCHandle(SQL_HANDLE_STMT, _stmt);
            SET_LAST_ERROR2(msg.arg(i), QSqlError::StatementError, detail, code)
            setAt(QSql::AfterLastRow);
            return false;
        }

        switch (colType)
        {
            case SQL_BIT:       // [bit]
                 row[idx] = bitData(_stmt, i);
                break;

            case SQL_BIGINT:    // [bigint]
                row[idx] = bigIntData(_stmt, i);
                break;

            case SQL_TINYINT:   // [tinyint]
            case SQL_SMALLINT:  // [smallint]
            case SQL_INTEGER:   // [int]
                row[idx] = intData(_stmt, i);
                break;

            case SQL_TYPE_DATE: // [date]
                DATE_STRUCT dbuf;
                rc = SQLGetData(_stmt, i + 1, SQL_C_DATE, &dbuf, 0, &lenIndicator);
                if (SQL_SUCCEEDED(rc) && (lenIndicator != SQL_NULL_DATA))
                    row[idx] = QVariant(QDate(dbuf.year, dbuf.month, dbuf.day));
                else
                    row[idx] = QVariant(QVariant::Date);
                break;

            case SQL_SS_TIME2:  // [time](7)
                TIME_STRUCT tbuf;
                rc = SQLGetData(_stmt, i + 1, SQL_C_TIME, &tbuf, 0, &lenIndicator);
                if (SQL_SUCCEEDED(rc) && (lenIndicator != SQL_NULL_DATA))
                    row[idx] = QVariant(QTime(tbuf.hour, tbuf.minute, tbuf.second));
                else
                    row[idx] = QVariant(QVariant::Time);
                break;

            case SQL_TYPE_TIMESTAMP:     // [datetime], [datetime2](7), [smalldatetime]
            case SQL_SS_TIMESTAMPOFFSET: // [datetimeoffset](7)
                TIMESTAMP_STRUCT dtbuf;
                rc = SQLGetData(_stmt, i + 1, SQL_C_TIMESTAMP, &dtbuf, 0, &lenIndicator);
                if (SQL_SUCCEEDED(rc) && (lenIndicator != SQL_NULL_DATA))
                {
                    QDateTime dt {QDate(dtbuf.year, dtbuf.month, dtbuf.day),
                                  QTime(dtbuf.hour, dtbuf.minute, dtbuf.second,
                                        dtbuf.fraction / 1000000)};
                    row[idx] = QVariant(dt);
                }
                else
                    row[idx] = QVariant(QVariant::DateTime);
                break;

            case SQL_GUID:      // [uniqueidentifier]
                row[idx] = guidData(_stmt, i);
                break;

            case SQL_VARBINARY: // [varbinary](n), [varbinary](max)
            case SQL_BINARY:    // [binary](n), [timestamp]
                row[idx] = binaryData(_stmt, i);
                break;


            case SQL_CHAR:      // [char](n)
            case SQL_WCHAR:     // [nchar](10)
            case SQL_WVARCHAR:  // [nvarchar](50), [nvarchar](max)
            case SQL_VARCHAR:   // [varchar](n)
            {
                if (_drv->_wideChar)
                    row[idx] = stringDataW(_stmt, i, colSize);
                else
                    row[idx] = stringDataA(_stmt, i, colSize);

                break;
            }

            case SQL_NUMERIC:   // [numeric](18, 0)
            case SQL_DECIMAL:   // [decimal](18, 0)
                row[idx] = numericData(_stmt, i);
                break;

            case SQL_REAL:      // [real]
            case SQL_FLOAT:     // [float]
                switch (numericalPrecisionPolicy())
                {
                    case QSql::LowPrecisionInt32:
                        row[idx] = intData(_stmt, i);
                        break;

                    case QSql::LowPrecisionInt64:
                        row[idx] = bigIntData(_stmt, i);
                        break;

                    case QSql::LowPrecisionDouble:
                        row[idx] = doubleData(_stmt, i);
                        break;
                    case QSql::HighPrecision:
                        row[idx] = stringDataA(_stmt, i, colSize);
                        break;
                }
                break;

            default:
                row[idx] = QVariant();
        }
    } // for (int i = 0; i < nfields; ++i)

    _numRowsAffected += 1;
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
    // Характеристика DriverFeature::QuerySize не может быть полноценно
    // реализована для этого  драйвера,  поэтому  метод  size()  должен
    // возвращать -1
    return -1;
}

int Result::size2(const DriverPtr& drv) const
{
    if (!isSelectSql() || _preparedQuery.isEmpty())
    {
        log_error_m << "Size of result unavailable"
                    << ". Detail: Sql-statement not SELECT or not prepared";
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
}

int Result::numRowsAffected()
{
    if (isSelectSql())
    {
        log_debug_m << "SQLRowCount returns the number of rows affected"
                       " by an UPDATE, INSERT, or DELETE statement";
        return _numRowsAffected;
    }

    SQLLEN affectedRowCount = 0;
    SQLRETURN rc = SQLRowCount(_stmt, &affectedRowCount);
    if (CHECK_ERROR("Failed get number affected rows", QSqlError::UnknownError))
        return -1;

    return affectedRowCount;
}

QSqlRecord Result::record() const
{
    if (!isActive() || !isSelectSql())
        return {};

    #define RECORD_PRINT_ERROR(MSG) { \
        auto [detail, code] = warnODBCHandle(SQL_HANDLE_STMT, _stmt); \
        log_error_m << log_format( \
            "%?. Transact: %?/%?. Detail: %?. Error code: %?", \
            MSG, addrToNumber(_drv->_connect), transactId(), detail, code); }

    SQLSMALLINT nfields;
    SQLRETURN rc = SQLNumResultCols(_stmt, &nfields);
    if (!SQL_SUCCEEDED(rc))
    {
        RECORD_PRINT_ERROR("Failed get fields count")
        return {};
    }

    QSqlRecord rec;
    for (int i = 0; i < nfields; ++i)
    {
        ushort colName[COL_NAME_SIZE] = {0};
        SQLSMALLINT colNameLen;
        SQLSMALLINT colType;
        SQLULEN colSize;
        SQLSMALLINT colScale;
        SQLSMALLINT nullable;

        SQLRETURN rc;
        rc = SQLDescribeColW(_stmt, i + 1, colName, (SQLSMALLINT)COL_NAME_SIZE,
                             &colNameLen, &colType, &colSize, &colScale, &nullable);
        if (!SQL_SUCCEEDED(rc))
        {
            QString msg = "Failed get describe for column %1";
            RECORD_PRINT_ERROR(msg.arg(i))
            return {};
        }

        SQLLEN unsignedFlag = SQL_FALSE;
        rc = SQLColAttribute(_stmt, i + 1, SQL_DESC_UNSIGNED, 0, 0, 0,
                             &unsignedFlag);
        if (!SQL_SUCCEEDED(rc))
        {
            QString msg = "Failed get 'unsigned' attribute for column %1";
            RECORD_PRINT_ERROR(msg.arg(i))
            return {};
        }

        SQLLEN autoIncrement = 0; // Check for auto-increment
        rc = SQLColAttribute(_stmt, i + 1, SQL_DESC_AUTO_UNIQUE_VALUE, 0, 0, 0,
                             &autoIncrement);
        if (!SQL_SUCCEEDED(rc))
        {
            QString msg = "Failed get 'auto increment' attribute for column %1";
            RECORD_PRINT_ERROR(msg.arg(i))
            return {};
        }

        ushort tableName[TABLE_NAME_SIZE] = {0};
        SQLSMALLINT tableNameLen;
        rc = SQLColAttribute(_stmt, i + 1, SQL_DESC_BASE_TABLE_NAME,
                             tableName, TABLE_NAME_SIZE, &tableNameLen, 0);
        if (!SQL_SUCCEEDED(rc))
        {
            QString msg = "Failed get name of table for column %1";
            RECORD_PRINT_ERROR(msg.arg(i))
            return {};
        }

        QString fieldName = QString::fromUtf16(colName, colNameLen).trimmed();
        QVariant::Type fieldType = decodeODBCType(colType, (unsignedFlag == SQL_FALSE));
        if (fieldType == QVariant::Invalid)
        {
            log_error_m << "Unknown field type"
                        << ". Field name: " << fieldName
                        << ". Column type: " << colType;
        }
        QSqlField f {fieldName, fieldType};

        f.setLength(colSize);
        f.setPrecision(colScale);
        f.setSqlType(colType);
        f.setRequired(nullable == SQL_NO_NULLS);
        f.setAutoValue(autoIncrement);
        f.setTableName(QString::fromUtf16(tableName, tableNameLen).trimmed());

        rec.append(f);
    }

    #undef RECORD_PRINT_ERROR

    return rec;
}

//-------------------------------- Driver ------------------------------------

#define DRIVER_PRINT_ERROR(MSG, HANDLE_TYPE, HANDLE) { \
    setLastError(QSqlError("MssqlDriver", MSG, QSqlError::ConnectionError, "1")); \
    auto [detail, code] = warnODBCHandle(HANDLE_TYPE, HANDLE); \
    log_error_m << log_format("%?. Detail: %?. Error code: %?", \
                              MSG, detail, code); }

Driver::Driver() : QSqlDriver(nullptr)
{}

Driver::~Driver()
{
    close();
}

Driver::Ptr Driver::create()
{
    return Driver::Ptr(new Driver);
}

//static size_t qGetODBCVersion(const QString &connOpts)
//{
//    if (connOpts.contains("SQL_ATTR_ODBC_VERSION=SQL_OV_ODBC3", Qt::CaseInsensitive))
//        return SQL_OV_ODBC3;
//    return SQL_OV_ODBC2;
//}

//bool Driver::setConnectionOptions(const QString& connOpts)
//{
//    // Set any connection attributes
//    QStringList opts {connOpts.split(QChar(';'), QString::SkipEmptyParts)};
//    for (int i = 0; i < opts.count(); ++i)
//    {
//        QString tmp = opts.at(i);
//        int idx = tmp.indexOf(QChar('='));
//        if (idx == -1)
//        {
//            log_warn_m <<"QODBCDriver::open: Illegal connect option value '" << tmp << '\'';
//            continue;
//        }
//        SQLUINTEGER v = 0;
//        QString opt {tmp.left(idx).toUpper()};
//        QString val {tmp.mid(idx + 1).simplified().toUpper()};

//        SQLRETURN rc = SQL_SUCCESS;
//        if (opt == "SQL_ATTR_ACCESS_MODE")
//        {
//            if (val == "SQL_MODE_READ_ONLY")
//            {
//                v = SQL_MODE_READ_ONLY;
//            }
//            else if (val == "SQL_MODE_READ_WRITE")
//            {
//                v = SQL_MODE_READ_WRITE;
//            }
//            else
//            {
//                log_warn_m << "QODBCDriver::open: Unknown option value '" << val << '\'';
//                continue;
//            }
//            rc = SQLSetConnectAttr(_connect, SQL_ATTR_ACCESS_MODE, (SQLPOINTER) size_t(v), 0);
//        }
//        else if (opt == "SQL_ATTR_CONNECTION_TIMEOUT")
//        {
//            v = val.toUInt();
//            rc = SQLSetConnectAttr(_connect, SQL_ATTR_CONNECTION_TIMEOUT, (SQLPOINTER) size_t(v), 0);
//        }
//        else if (opt == "SQL_ATTR_LOGIN_TIMEOUT")
//        {
//            v = val.toUInt();
//            rc = SQLSetConnectAttr(_connect, SQL_ATTR_LOGIN_TIMEOUT, (SQLPOINTER) size_t(v), 0);
//        }
//        else if (opt == "SQL_ATTR_CURRENT_CATALOG")
//        {
//            val.utf16(); // 0 terminate
//            rc = SQLSetConnectAttr(_connect, SQL_ATTR_CURRENT_CATALOG,
//                                   (SQLPOINTER*)val.utf16(),
//                                   val.length() * sizeof(ushort));
//        }
//        else if (opt == "SQL_ATTR_METADATA_ID")
//        {
//            if (val == "SQL_TRUE")
//            {
//                v = SQL_TRUE;
//            }
//            else if (val == "SQL_FALSE")
//            {
//                v = SQL_FALSE;
//            }
//            else
//            {
//                log_warn_m << "QODBCDriver::open: Unknown option value '" << val << '\'';
//                continue;
//            }
//            rc = SQLSetConnectAttr(_connect, SQL_ATTR_METADATA_ID, (SQLPOINTER) size_t(v), 0);
//        }
//        else if (opt == "SQL_ATTR_PACKET_SIZE")
//        {
//            v = val.toUInt();
//            rc = SQLSetConnectAttr(_connect, SQL_ATTR_PACKET_SIZE, (SQLPOINTER) size_t(v), 0);
//        }
//        else if (opt == "SQL_ATTR_TRACEFILE")
//        {
//            val.utf16(); // 0 terminate
//            rc = SQLSetConnectAttr(_connect, SQL_ATTR_TRACEFILE,
//                                    (SQLPOINTER*)val.utf16(),
//                                    val.length() * sizeof(ushort));
//        }
//        else if (opt == "SQL_ATTR_TRACE")
//        {
//            if (val == "SQL_OPT_TRACE_OFF")
//            {
//                v = SQL_OPT_TRACE_OFF;
//            }
//            else if (val == "SQL_OPT_TRACE_ON")
//            {
//                v = SQL_OPT_TRACE_ON;
//            } else {
//                log_warn_m << "QODBCDriver::open: Unknown option value '" << val << '\'';
//                continue;
//            }
//            rc = SQLSetConnectAttr(_connect, SQL_ATTR_TRACE, (SQLPOINTER) size_t(v), 0);
//        }
//        else if (opt == "SQL_ATTR_CONNECTION_POOLING")
//        {
//            if (val == "SQL_CP_OFF")
//                v = SQL_CP_OFF;
//            else if (val == "SQL_CP_ONE_PER_DRIVER")
//                v = SQL_CP_ONE_PER_DRIVER;
//            else if (val == "SQL_CP_ONE_PER_HENV")
//                v = SQL_CP_ONE_PER_HENV;
//            else if (val == "SQL_CP_DEFAULT")
//                v = SQL_CP_DEFAULT;
//            else
//            {
//                log_warn_m << "QODBCDriver::open: Unknown option value '" << val << '\'';
//                continue;
//            }
//            rc = SQLSetConnectAttr(_connect, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER) size_t(v), 0);
//        }
//        else if (opt == "SQL_ATTR_CP_MATCH")
//        {
//            if (val == "SQL_CP_STRICT_MATCH")
//                v = SQL_CP_STRICT_MATCH;
//            else if (val == "SQL_CP_RELAXED_MATCH")
//                v = SQL_CP_RELAXED_MATCH;
//            else if (val == "SQL_CP_MATCH_DEFAULT")
//                v = SQL_CP_MATCH_DEFAULT;
//            else
//            {
//                log_warn_m << "QODBCDriver::open: Unknown option value '" << val << '\'';
//                continue;
//            }
//            rc = SQLSetConnectAttr(_connect, SQL_ATTR_CP_MATCH, (SQLPOINTER) size_t(v), 0);
//        }
//        else if (opt == "SQL_ATTR_ODBC_VERSION")
//        {
//            // Already handled in QODBCDriver::open()
//            continue;
//        }
//        else
//        {
//            log_warn_m << "QODBCDriver::open: Unknown connection attribute '" << opt << '\'';
//        }
//        if (!SQL_SUCCEEDED(rc))
//            PRINT_ERROR(QString("QODBCDriver::open: Unable to set connection attribute'%1'").arg(opt), this);
//    }
//    return true;
//}

bool Driver::open(const QString& db,
                  const QString& user,
                  const QString& password,
                  const QString& host,
                  int   port,
                  const QString& connOpts)
{
    if (isOpen())
      close();

    auto freeResources = [this]()
    {
        if (_connect)
        {
            if (!SQL_SUCCEEDED(SQLFreeHandle(SQL_HANDLE_DBC, _connect)))
            {
                QString msg = "Failed free the connection handle";
                DRIVER_PRINT_ERROR(msg, SQL_HANDLE_DBC, _connect)
            }
        }
        if (_env)
        {
            if (!SQL_SUCCEEDED(SQLFreeHandle(SQL_HANDLE_ENV, _env)))
            {
                QString msg = "Failed free the environment handle";
                DRIVER_PRINT_ERROR(msg, SQL_HANDLE_DBC, _connect)
            }
        }
        _connect = nullptr;
        _env = nullptr;
    };

    SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &_env);
    if (!SQL_SUCCEEDED(rc))
    {
        QString msg = "Failed to allocate environment handle";
        DRIVER_PRINT_ERROR(msg, SQL_HANDLE_ENV, _env)
        setOpenError(true);
        return false;
    }

    rc = SQLSetEnvAttr(_env, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_UINTEGER);
    if (!SQL_SUCCEEDED(rc))
    {
        QString msg = "Failed set ODBC version to 3";
        DRIVER_PRINT_ERROR(msg, SQL_HANDLE_ENV, _env)
        setOpenError(true);
        freeResources();
        return false;
    }

    rc = SQLAllocHandle(SQL_HANDLE_DBC, _env, &_connect);
    if (!SQL_SUCCEEDED(rc))
    {
        QString msg = "Failed to allocate connection handle";
        DRIVER_PRINT_ERROR(msg, SQL_HANDLE_DBC, _connect)
        setOpenError(true);
        freeResources();
        return false;
    }

    QString connString;
    connString += QString("Server=%1,%2").arg(host).arg(port);
    connString += QString(";Database=%1").arg(db);

    if (!user.isEmpty())
        connString += QString(";UID=%1").arg(user);

    if (!password.isEmpty())
        connString += QString(";PWD=%1").arg(password);

    connString += ";" + connOpts;

    { //Block for alog::Line
        alog::Line logLine = log_verbose_m << "Try open database '" << db << "'"
                                           << ". User: " << user
                                           << ", host: " << host
                                           << ", port: " << port;
        if (!connOpts.trimmed().isEmpty())
            logLine << ", options: " << connOpts.trimmed();
    }

    for (const QString& connOpt : connOpts.split(QChar(';'), QString::SkipEmptyParts))
    {
        QStringList option = connOpt.split(QChar('='));
        if (option.size() != 2)
            continue;

        if (option[0].trimmed() == "Connection Timeout")
        {
            bool ok;
            long timeout = option[1].toLong(&ok);
            if (!ok)
            {
                QString msg = "Incorrect option 'Connection Timeout', param must be numeric";
                DRIVER_PRINT_ERROR(msg, SQL_HANDLE_DBC, _connect)
                setOpenError(true);
                freeResources();
                return false;
            }

            rc = SQLSetConnectAttrW(_connect, SQL_ATTR_LOGIN_TIMEOUT,
                                    (SQLPOINTER)timeout, SQL_IS_INTEGER);
            if (!SQL_SUCCEEDED(rc))
            {
                QString msg = "Failed set option 'Connection Timeout'";
                DRIVER_PRINT_ERROR(msg, SQL_HANDLE_DBC, _connect)
                setOpenError(true);
                freeResources();
                return false;
            }
        }
    }

    SQLSMALLINT cb;
    QVarLengthArray<ushort> connOut {1024};
    memset(connOut.data(), 0, connOut.size() * sizeof(ushort));

    rc = SQLDriverConnectW(_connect, NULL,
                           (SQLWCHAR*)connString.unicode(), connString.length(),
                           connOut.data(), connOut.length(), &cb, /*SQL_DRIVER_NOPROMPT*/0);
    if (!SQL_SUCCEEDED(rc))
    {
        QString msg = "Error opening database";
        DRIVER_PRINT_ERROR(msg, SQL_HANDLE_DBC, _connect)
        setOpenError(true);
        freeResources();
        return false;
    }

    rc = SQLSetConnectAttr(_connect, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);
    if (!SQL_SUCCEEDED(rc))
    {
        QString msg = "Failed to disable autocommit";
        DRIVER_PRINT_ERROR(msg, SQL_HANDLE_DBC, _connect)

        rc = SQLDisconnect(_connect);
        if (!SQL_SUCCEEDED(rc))
        {
            QString msg = "Failed to disconnect datasource";
            DRIVER_PRINT_ERROR(msg, SQL_HANDLE_DBC, _connect)
        }
        setOpenError(true);
        freeResources();
        return false;
    }

//    int StringLengthPtrc = 0;

//    rc = SQLGetConnectAttr(_dbc, SQL_ATTR_CURRENT_CATALOG, NULL, 0, &StringLengthPtr);
//    QByteArray catalog;
//    catalog.resize(StringLengthPtr+1);
//    rc = SQLGetConnectAttr(_dbc, SQL_ATTR_CURRENT_CATALOG, catalog.data(), StringLengthPtr+1, &StringLengthPtr);
//    _catalog = QString(catalog);

//    if (r != SQL_SUCCESS && r != SQL_SUCCESS_WITH_INFO)
//    {
//        setLastError(qMakeError(tr("Unable to connect"), QSqlError::ConnectionError, this));
//        setOpenError(true);
//        return false;
//    }

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

    SQLRETURN rc;
    SQLHANDLE connect = _connect;

    if (_connect)
    {
        rc = SQLDisconnect(_connect);
        if (!SQL_SUCCEEDED(rc))
        {
            QString msg = "Failed to disconnect datasource";
            DRIVER_PRINT_ERROR(msg, SQL_HANDLE_DBC, _connect)
        }

        rc = SQLFreeHandle(SQL_HANDLE_DBC, _connect);
        if (!SQL_SUCCEEDED(rc))
        {
            QString msg = "Failed to allocate environment handle";
            DRIVER_PRINT_ERROR(msg, SQL_HANDLE_DBC, _connect)
        }
    }

    if (_env)
    {
        rc = SQLFreeHandle(SQL_HANDLE_ENV, _env);
        if (!SQL_SUCCEEDED(rc))
        {
            QString msg = "Failed to allocate environment handle";
            DRIVER_PRINT_ERROR(msg, SQL_HANDLE_ENV, _env)
        }
    }

    _env = nullptr;
    _connect = nullptr;
    _threadId = 0;
    _transactNumber = 0;

    setOpen(false);
    setOpenError(false);

    log_verbose_m << "Database is closed. Connect: " << addrToNumber(connect);
}

bool Driver::isOpen() const
{
    return _connect;
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

        // Характеристика не может быть полноценно реализована  для  этого
        // драйвера. Количество записей для предварительно подготовленного
        // запроса можно получить при помощи функции resultSize()
        case QuerySize:
            return false;

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
    if (_transactNumber == 0)
    {
        _transactNumber = addrToNumber(transact);
        log_debug2_m << "Transaction address captured: " << addrToNumber(transact)
                     << ". Connect: " << addrToNumber(_connect);
    }
    else
        log_warn_m << "Failed capture transaction address: "  << addrToNumber(transact)
                   << ". Already captured: " << _transactNumber
                   << ". Connect: " << addrToNumber(_connect);
}

void Driver::releaseTransactAddr(Transaction* transact)
{
    if (_transactNumber == addrToNumber(transact))
    {
        _transactNumber = 0;
        log_debug2_m << "Transaction address released: " << addrToNumber(transact)
                     << ". Connect: " << addrToNumber(_connect);
    }
    else
        log_warn_m << "Failed release transaction address: "  << addrToNumber(transact)
                   << ". Already captured: " << _transactNumber
                   << ". Connect: " << addrToNumber(_connect);
}

bool Driver::transactAddrIsEqual(Transaction* transact)
{
    return (_transactNumber == addrToNumber(transact));
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

void Driver::abortOperation()
{
    log_verbose_m << "Abort sql-operation"
                  << ". Connect: " << addrToNumber(_connect)
                  << " (call from thread: " << trd::gettid() << ")";

    _operationIsAborted = true;
    emit abortStatement();
}

bool Driver::operationIsAborted() const
{
    return _operationIsAborted;
}

#undef DRIVER_PRINT_ERROR

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
