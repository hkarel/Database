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

#include "firebird_driver.h"

#include "shared/break_point.h"
#include "shared/safe_singleton.h"
#include "shared/logger/logger.h"
#include "shared/logger/format.h"
#include "shared/qt/quuidex.h"
#include "shared/qt/logger_operators.h"
#include "shared/thread/thread_utils.h"

#include <QDateTime>
#include <QVariant>
#include <QSqlField>
#include <QSqlIndex>
#include <QVarLengthArray>
#include <cstdlib>
#include <utility>

#define log_error_m   alog::logger().error   (alog_line_location, "FirebirdDrv")
#define log_warn_m    alog::logger().warn    (alog_line_location, "FirebirdDrv")
#define log_info_m    alog::logger().info    (alog_line_location, "FirebirdDrv")
#define log_verbose_m alog::logger().verbose (alog_line_location, "FirebirdDrv")
#define log_debug_m   alog::logger().debug   (alog_line_location, "FirebirdDrv")
#define log_debug2_m  alog::logger().debug2  (alog_line_location, "FirebirdDrv")

#define FBVERSION SQL_DIALECT_V6

#ifndef SQLDA_CURRENT_VERSION
#define SQLDA_CURRENT_VERSION SQLDA_VERSION1
#endif

namespace db {
namespace firebird {

namespace {

enum {FirebirdChunkSize = SHRT_MAX / 2};

bool firebirdError(const ISC_STATUS* status, const QTextCodec* tc,
                   ISC_LONG& sqlcode, QString& msg)
{
    if (status[0] != 1 || status[1] <= 0)
        return false;

    msg.clear();
    sqlcode = isc_sqlcode(status);
    char buf[512];
    while (fb_interpret(buf, 512, &status))
    {
        if (!msg.isEmpty())
            msg += QLatin1String(" - ");
        if (tc)
            msg += tc->toUnicode(buf);
        else
            msg += QString::fromUtf8(buf);
    }
    return true;
}

void deleteDA(XSQLDA*& sqlda)
{
    if (!sqlda)
        return;

    //for (int i = 0; i < sqlda->sqld; ++i)
    for (int i = 0; i < sqlda->sqln; ++i)
    {
        delete [] sqlda->sqlvar[i].sqlind;
        delete [] sqlda->sqlvar[i].sqldata;
    }
    free(sqlda);
    sqlda = 0;
}

bool createDA(XSQLDA*& sqlda)
{
    if (sqlda != (XSQLDA*)0)
        deleteDA(sqlda);

    sqlda = (XSQLDA*) malloc(XSQLDA_LENGTH(1));
    if (sqlda == (XSQLDA*)0)
    {
        log_error_m << "Call createDA(): failed to allocate memory";
        return false;
    }
    memset(sqlda, 0, XSQLDA_LENGTH(1));
    sqlda->sqln = 1;
    //sqlda->sqld = 0;
    sqlda->version = SQLDA_CURRENT_VERSION;
    //sqlda->sqlvar[0].sqlind = 0;
    //sqlda->sqlvar[0].sqldata = 0;
    return true;
}

bool enlargeDA(XSQLDA*& sqlda, int n)
{
    if (sqlda != (XSQLDA*)0)
        deleteDA(sqlda);

    sqlda = (XSQLDA*) malloc(XSQLDA_LENGTH(n));
    if (sqlda == (XSQLDA*)0)
    {
        log_error_m << "Call enlargeDA(): failed to allocate memory";
        return false;
    }
    memset(sqlda, 0, XSQLDA_LENGTH(n));
    sqlda->sqln = n;
    sqlda->version = SQLDA_CURRENT_VERSION;
    return true;
}

void initDA(XSQLDA* sqlda)
{
    for (int i = 0; i < sqlda->sqld; ++i)
    {
        switch (sqlda->sqlvar[i].sqltype & ~1)
        {
            case SQL_INT64:
            case SQL_LONG:
            case SQL_SHORT:
            case SQL_FLOAT:
            case SQL_DOUBLE:
            case SQL_TIMESTAMP:
            case SQL_TYPE_TIME:
            case SQL_TYPE_DATE:
            case SQL_TEXT:
            case SQL_BLOB:
                sqlda->sqlvar[i].sqldata = new char[sqlda->sqlvar[i].sqllen];
                break;

            case SQL_ARRAY:
                sqlda->sqlvar[i].sqldata = new char[sizeof(ISC_QUAD)];
                memset(sqlda->sqlvar[i].sqldata, 0, sizeof(ISC_QUAD));
                break;

            case SQL_VARYING:
                sqlda->sqlvar[i].sqldata = new char[sqlda->sqlvar[i].sqllen + sizeof(short)];
                break;

            default:
                // not supported - do not bind.
                sqlda->sqlvar[i].sqldata = 0;
                break;
        }
        if (sqlda->sqlvar[i].sqltype & 1)
        {
            sqlda->sqlvar[i].sqlind = new short[1];
            *(sqlda->sqlvar[i].sqlind) = 0;
        }
        else
            sqlda->sqlvar[i].sqlind = 0;
    }
}

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
        {
            // Отладить
            break_point
            return QVariant::ByteArray;
        }
        case blr_quad:
        case blr_short:
        case blr_long:
            return (hasScale) ? QVariant::Double : QVariant::Int;

        case blr_int64:
            return (hasScale) ? QVariant::Double : QVariant::LongLong;

        case blr_float:
            return QVariant::Type(qMetaTypeId<float>());

        case blr_d_float:
        case blr_double:
            return QVariant::Double;
    }
    log_warn_m << "qFirebirdTypeName(): unknown datatype: " << iType;
    return QVariant::Invalid;
}

QVariant::Type qFirebirdTypeName2(int iType, bool hasScale, int subType, int subLength)
{
    switch (iType & ~1)
    {
        case SQL_VARYING:
        case SQL_TEXT:
            // [Karelin]
            // return QVariant::String;
            // return (subType == 1 /*OCTET*/) ? QVariant::ByteArray : QVariant::String;
            if (subType == 1 /*OCTET*/)
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
            return QVariant::Type(qMetaTypeId<float>());

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
    }
    log_warn_m << "qFirebirdTypeName2(): unknown datatype: " << iType;
    return QVariant::Invalid;
}

ISC_TIMESTAMP toTimeStamp(const QDateTime& dt)
{
    static const QTime midnight {0, 0, 0, 0};
    static const QDate basedate {1858, 11, 17};
    ISC_TIMESTAMP ts;
    ts.timestamp_time = midnight.msecsTo(dt.time()) * 10;
    ts.timestamp_date = basedate.daysTo(dt.date());
    return ts;
}

QDateTime fromTimeStamp(const char* buffer)
{
    static const QTime midnight {0, 0, 0, 0};
    static const QDate basedate {1858, 11, 17};

    // have to demangle the structure ourselves because isc_decode_time
    // strips the msecs
    QTime t = midnight.addMSecs(int(((ISC_TIMESTAMP*)buffer)->timestamp_time / 10));
    QDate d = basedate.addDays (int(((ISC_TIMESTAMP*)buffer)->timestamp_date));
    return QDateTime(d, t);
}

ISC_TIME toTime(const QTime& t)
{
    static const QTime midnight {0, 0, 0, 0};
    return (ISC_TIME)midnight.msecsTo(t) * 10;
}

QTime fromTime(const char* buffer)
{
    static const QTime midnight {0, 0, 0, 0};
    // have to demangle the structure ourselves because isc_decode_time
    // strips the msecs
    QTime t = midnight.addMSecs(int((*(ISC_TIME*)buffer) / 10));
    return t;
}

ISC_DATE toDate(const QDate& t)
{
    static const QDate basedate {1858, 11, 17};
    ISC_DATE date = basedate.daysTo(t);
    return date;
}

QDate fromDate(const char* buffer)
{
    static const QDate basedate {1858, 11, 17};
    // have to demangle the structure ourselves because isc_decode_time
    // strips the msecs
    QDate d = basedate.addDays(int(((ISC_TIMESTAMP*)buffer)->timestamp_date));
    return d;
}

QByteArray encodeString(const QTextCodec* tc, const QString& str)
{
    return (tc) ? tc->fromUnicode(str) : str.toUtf8();
}

template<typename T>
QList<QVariant> toList(char** buf, int count)
{
    QList<QVariant> res;
    for (int i = 0; i < count; ++i)
    {
        T value = *(T*)(*buf);
        res.append(value);
        *buf += sizeof(T);
    }
    return res;
}

/* char** ? seems like bad influence from oracle ... */
QList<QVariant> toListLong(char** buf, int count)
{
    QList<QVariant> res;
    for (int i = 0; i < count; ++i)
    {
        if (sizeof(int) == sizeof(long))
            res.append(int(*(long*)(*buf)));
        else
            res.append(qint64(*(long*)(*buf)));

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
                /* FALLTHRU - reserved words for fix GCC 7 warning */

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
                valList = toListLong(&buffer, numElements[dim]);
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
        // Отладить
        break_point
        list.append(valList);
    }
    else
    {
        // Отладить
        break_point
        list += valList;
    }
    return buffer;
}

template<typename T>
char* fillList(char* buffer, const QList<QVariant>& list)
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

char* fillListFloat(char* buffer, const QList<QVariant>& list)
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

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wswitch-enum"

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
        // Отладить
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
                    buffer = fillListFloat(buffer, list);
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

#pragma GCC diagnostic pop

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

} // namespace

//------------------------------- Transaction --------------------------------

Transaction::Transaction(const DriverPtr& drv) : _drv(drv)
{
    log_debug2_m << "Transaction ctor";
    Q_ASSERT(_drv.get());
}

Transaction::~Transaction()
{
    log_debug2_m << "Transaction dtor";
    if (isActive())
        rollback();
}

bool Transaction::begin()
{
    if (_drv->operationIsAborted())
    {
        log_error_m << "Failed begin transaction, sql-operation aborted"
                    << ". Connect: " << _drv->_ibase;
        return false;
    }
    if (!_drv->isOpen() || _drv->isOpenError())
    {
        log_error_m << "Failed begin transaction, database not open";
        return false;
    }
    if (_trans)
    {
        log_error_m << "Transaction already begun: " << _drv->_ibase << "/" << _trans;
        return false;
    }
    ISC_STATUS status[20] = {0};
    isc_start_transaction(status, &_trans, 1, _drv->ibase(), 0, NULL);

    ISC_LONG sqlcode; QString msg;
    if (firebirdError(status, _drv->_textCodec, sqlcode, msg))
    {
        log_error_m << "Failed begin transaction. Connect: " << _drv->_ibase
                    << ". Detail: " << msg << "; SqlCode: " << sqlcode;

        // Прерываем использование данного подключения
        _drv->abortOperation();
        return false;
    }
    log_debug2_m << "Transaction begin: " << _drv->_ibase << "/" << _trans;
    return true;
}

bool Transaction::commit()
{
    if (_drv->operationIsAborted())
    {
        log_error_m << "Failed commit transaction, sql-operation aborted"
                    << ". Connect: " << _drv->_ibase;
        return false;
    }
    if (!_drv->isOpen() || _drv->isOpenError())
    {
        log_error_m << "Failed commit transaction, database not open";
        return false;
    }
    if (!_trans)
    {
        log_error_m << "Failed commit transaction, transaction not begun"
                    << ". Connect: " << _drv->_ibase;
        return false;
    }
    ISC_STATUS status[20] = {0};
    isc_tr_handle trans = _trans;
    isc_commit_transaction(status, &_trans);
    _trans = 0;

    ISC_LONG sqlcode; QString msg;
    if (firebirdError(status, _drv->_textCodec, sqlcode, msg))
    {
        log_error_m << "Failed commit transaction: " << _drv->_ibase << "/" << trans
                    << ". Detail: " << msg << "; SqlCode: " << sqlcode;
        return false;
    }
    log_debug2_m << "Transaction commit: " << _drv->_ibase << "/" << trans;
    return true;
}

bool Transaction::rollback()
{
    if (!_drv->isOpen() || _drv->isOpenError())
    {
        log_error_m << "Failed rollback transaction, database not open";
        return false;
    }
    if (!_trans)
    {
        log_error_m << "Failed rollback transaction, transaction not begun"
                    << ". Connect: " << _drv->_ibase;
        return false;
    }
    ISC_STATUS status[20] = {0};
    isc_tr_handle trans = _trans;
    isc_rollback_transaction(status, &_trans);
    _trans = 0;

    ISC_LONG sqlcode; QString msg;
    if (firebirdError(status, _drv->_textCodec, sqlcode, msg))
    {
        log_error_m << "Failed rollback transaction: " << _drv->_ibase << "/" << trans
                    << ". Detail: " << msg << "; SqlCode: " << sqlcode;
        return false;
    }
    log_debug2_m << "Transaction rollback: " << _drv->_ibase << "/" << trans;
    return true;
}

bool Transaction::isActive() const
{
    return bool(_trans);
}

AutoRollbackTransact::AutoRollbackTransact(const Transaction::Ptr& t)
    : transact(t)
{}

AutoRollbackTransact::~AutoRollbackTransact()
{
    log_debug2_m << "AutoRollbackTransact dtor";
    if (transact->isActive())
        transact->rollback();
}

//---------------------------------- Result ----------------------------------

#define CHECK_ERROR(MSG, ERR_TYPE) \
    checkError(MSG, ERR_TYPE, status, __func__, __LINE__)

#define SET_LAST_ERROR(MSG, ERR_TYPE) { \
    setLastError(QSqlError("FirebirdResult", MSG, ERR_TYPE, 1)); \
    alog::logger().error(alog_line_location, "FirebirdDrv") << MSG; \
}

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
}

bool Result::checkError(const char* msg, QSqlError::ErrorType type,
                        ISC_STATUS* status, const char* func, int line)
{
    ISC_LONG sqlcode; QString err;
    if (firebirdError(status, _drv->_textCodec, sqlcode, err))
    {
        setLastError(QSqlError("FirebirdResult", msg, type, 1));
        alog::logger().error(alog::detail::file_name(__FILE__), func, line, "FirebirdDrv")
            << msg
            << ". Transact: " << _drv->_ibase << "/" << *transact()
            << ". Detail: "   << err
            << ". SqlCode: "  << sqlcode;
        return true;
    }
    return false;
}

void Result::cleanup()
{
    log_debug2_m << "Begin dataset cleanup. Connect: " << _drv->_ibase;

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
        ISC_STATUS status[20] = {0};
        isc_dsql_free_statement(status, &_stmt, DSQL_drop);
        CHECK_ERROR("Failed free statement", QSqlError::StatementError);
        _stmt = 0;
    }

    deleteDA(_sqlda);
    deleteDA(_inda);

    _queryType = -1;
    _preparedQuery.clear();
    SqlCachedResult::cleanup();

    log_debug2_m << "End dataset cleanup. Connect: " << _drv->_ibase;
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

isc_tr_handle* Result::transact() const
{
    if (_externalTransact)
        return _externalTransact->handle();

    if (_internalTransact)
        return _internalTransact->handle();

    return 0;
}

bool Result::prepare(const QString& query)
{
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

    if (!createDA(_sqlda))
        return false;

    if (!createDA(_inda))
        return false;

    if (!beginInternalTransact())
        return false;

    if (alog::logger().level() == alog::Level::Debug2)
    {
        QString sql = query;
        static QRegExp reg {R"(\s{2,})"};
        sql.replace(reg, " ");
        sql.replace(" ,", ",");
        if (!sql.isEmpty() && (sql[0] == QChar(' ')))
            sql.remove(0, 1);
        log_debug2_m << "Begin prepare query"
                     << ". Transact: " << _drv->_ibase << "/" << *transact()
                     << ". " << sql;
    }

    ISC_STATUS status[20] = {0};
    isc_dsql_allocate_statement(status, _drv->ibase(), &_stmt);
    if (CHECK_ERROR("Could not allocate statement", QSqlError::StatementError))
    {
        rollbackInternalTransact();
        return false;
    }

    QByteArray qstr = encodeString(_drv->_textCodec, query);
    isc_dsql_prepare(status, transact(), &_stmt, 0, qstr.constData(),
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
    _preparedQuery = query;

    log_debug2_m << "End prepare query"
                 << ". Transact: " << _drv->_ibase << "/" << *transact();
    return true;
}

bool Result::exec()
{
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
                 << ". Transact: " << _drv->_ibase << "/" << *transact();

    int i = 0;
    bool ok = true;
    setActive(false);
    setAt(QSql::BeforeFirstRow);

    if (_inda)
    {
        const QVector<QVariant>& values = boundValues();
        if (alog::logger().level() == alog::Level::Debug2)
        {
            for (i = 0; i < values.count(); ++i)
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
        for (i = 0; i < values.count(); ++i)
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
                QString msg = "Query param%1 is invalid. Transact: %2/%3";
                msg = msg.arg(i)
                         .arg(_drv->_ibase)
                         .arg(*transact());
                SET_LAST_ERROR(msg, QSqlError::StatementError)
                rollbackInternalTransact();
                return false;
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
                    //*((char*)d->inda->sqlvar[para].sqldata) = 0;
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
                    if (sqlVar.sqlsubtype == 1 /*OCTET*/)
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
                {
                    QString msg = "Query param%1, is unknown datatype: %2"
                                  ". Transact: %3/%4";
                    msg = msg.arg(i)
                             .arg(int(sqlVar.sqltype & ~1))
                             .arg(_drv->_ibase)
                             .arg(*transact());
                    SET_LAST_ERROR(msg, QSqlError::StatementError)
                    rollbackInternalTransact();
                    return false;
                }
            }
        }
    } // if (_inda)

    if (!ok)
    {
        QString msg = "Query param%1 is invalid. Transact: %2/%3";
        msg = msg.arg(i)
                 .arg(_drv->_ibase)
                 .arg(*transact());
        SET_LAST_ERROR(msg, QSqlError::StatementError)
        rollbackInternalTransact();
        return false;
    }

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

    isc_tr_handle trans = *transact();

    if (!isSelectSql())
        if (!commitInternalTransact())
        {
            log_debug2_m << "Failed exec query"
                         << ". Transact: " << _drv->_ibase << "/" << trans;
            return false;
        }

    setActive(true);

    log_debug2_m << "End exec query"
                 << ". Transact: " << _drv->_ibase << "/" << trans;
    return true;
}

QVariant Result::fetchBlob(ISC_QUAD* bId)
{
    isc_blob_handle handle = 0;
    ISC_STATUS status[20] = {0};
    isc_open_blob2(status, _drv->ibase(), transact(), &handle, bId, 0, 0);
    if (CHECK_ERROR("Unable to open BLOB", QSqlError::StatementError))
        return QVariant();

    unsigned short len = 0;
    QByteArray ba;
    int chunkSize = FirebirdChunkSize;
    ba.resize(chunkSize);
    int read = 0;
    // [Karelin]
    // while (isc_get_segment(status,& handle,& len, chunkSize, ba.data() + read) == 0 || status[1] == isc_segment) {
    while (isc_get_segment(status, &handle, &len, chunkSize, (char*)ba.constData() + read) == 0
           || status[1] == isc_segment)
    {
        read += len;
        ba.resize(read + chunkSize);
    }
    ba.resize(read);

    bool isErr = false;
    if (status[1] != isc_segstr_eof)
        if (CHECK_ERROR("Unable to read BLOB", QSqlError::StatementError))
            isErr = true;

    isc_close_blob(status, &handle);

    if (isErr)
        return QVariant();

    ba.resize(read);
    return ba;
}

bool Result::writeBlob(XSQLVAR& sqlVar, const QByteArray& ba)
{
    isc_blob_handle handle = 0;
    ISC_STATUS status[20] = {0};
    ISC_QUAD* bId = (ISC_QUAD*)sqlVar.sqldata;
    isc_create_blob2(status, _drv->ibase(), transact(), &handle, bId, 0, 0);
    if (CHECK_ERROR("Unable to create BLOB", QSqlError::StatementError))
    {
        isc_close_blob(status, &handle);
        //CHECK_ERROR("Unable to close BLOB", QSqlError::StatementError);
        return false;
    }

    int j = 0;
    while (j < ba.size())
    {
        isc_put_segment(status, &handle, qMin(ba.size() - j, int(FirebirdChunkSize)),
                        // [Karelin]
                        // const_cast<char*>(ba.data()) + i);
                        ba.constData() + j);

        if (CHECK_ERROR("Unable to write BLOB", QSqlError::UnknownError))
            return false;

        j += qMin(ba.size() - j, int(FirebirdChunkSize));
    }
    isc_close_blob(status, &handle);
    return true;
}

QVariant Result::fetchArray(XSQLVAR& sqlVar, ISC_QUAD* arr)
{
    QList<QVariant> list;
    ISC_ARRAY_DESC desc;
    ISC_STATUS status[20] = {0};

    if (!arr)
        return list;

    QByteArray relname {sqlVar.relname,   sqlVar.relname_length};
    QByteArray sqlname {sqlVar.aliasname, sqlVar.aliasname_length};

    // [Karelin]
    // isc_array_lookup_bounds(status,& ibase,& trans, relname.data(), sqlname.data(),& desc);
    break_point

    isc_array_lookup_bounds(status, _drv->ibase(), transact(),
                            relname.constData(), sqlname.constData(), &desc);
    if (CHECK_ERROR("Could not find array", QSqlError::StatementError))
        return list;

    int arraySize = 1, subArraySize;
    short dimensions = desc.array_desc_dimensions;
    QVarLengthArray<short> numElements; // {int(dimensions)};
    numElements.resize(dimensions);

    for (short i = 0; i < dimensions; ++i)
    {
        subArraySize = (desc.array_desc_bounds[i].array_bound_upper -
                        desc.array_desc_bounds[i].array_bound_lower + 1);
        numElements[i] = subArraySize;
        arraySize = subArraySize*  arraySize;
    }

    ISC_LONG bufLen;
    QByteArray ba;
    // Varying arrayelements are stored with 2 trailing null bytes
    // indicating the length of the string
    if (desc.array_desc_dtype == blr_varying
        || desc.array_desc_dtype == blr_varying2)
    {
        desc.array_desc_length += 2;
        bufLen = desc.array_desc_length * arraySize * sizeof(short);
    }
    else
        bufLen = desc.array_desc_length * arraySize;

    ba.resize(int(bufLen));
    // [Karelin]
    // isc_array_get_slice(status,& ibase,& trans, arr,& desc, ba.data(),& bufLen);
    break_point

    isc_array_get_slice(status, _drv->ibase(), transact(), arr, &desc,
                        (char*)ba.constData(), &bufLen);
    if (CHECK_ERROR("Could not get array data", QSqlError::StatementError))
        return list;

    // [Karelin]
    // readArrayBuffer(list, ba.data(), 0, numElements.data(),& desc, tc);
    break_point
    readArrayBuffer(list, (char*)ba.constData(), 0, (short*)numElements.constData(),
                    &desc, _drv->_textCodec);

    return QVariant(list);
}

bool Result::writeArray(XSQLVAR& sqlVar, const QList<QVariant>& list)
{
    QString error;
    ISC_QUAD* arrayId = (ISC_QUAD*)sqlVar.sqldata;
    ISC_ARRAY_DESC desc;
    ISC_STATUS status[20] = {0};

    QByteArray relname {sqlVar.relname,   sqlVar.relname_length};
    QByteArray sqlname {sqlVar.aliasname, sqlVar.aliasname_length};

    // [Karelin]
    // isc_array_lookup_bounds(status,& ibase,& trans, relname.data(), sqlname.data(),& desc);
    break_point
    isc_array_lookup_bounds(status, _drv->ibase(), transact(),
                            relname.constData(), sqlname.constData(), &desc);
    if (CHECK_ERROR("Could not find array", QSqlError::StatementError))
        return false;

    short arraySize = 1;
    ISC_LONG bufLen;

    short dimensions = desc.array_desc_dimensions;
    for(int i = 0; i < dimensions; ++i)
    {
        arraySize *= (desc.array_desc_bounds[i].array_bound_upper -
                      desc.array_desc_bounds[i].array_bound_lower + 1);
    }

    // Varying arrayelements are stored with 2 trailing null bytes
    // indicating the length of the string
    if (desc.array_desc_dtype == blr_varying
        || desc.array_desc_dtype == blr_varying2)
    {
        desc.array_desc_length += 2;
    }
    bufLen = desc.array_desc_length * arraySize;
    QByteArray ba;
    ba.resize(int(bufLen));

    if (list.size() > arraySize)
    {
        error = QLatin1String("Array size missmatch: size of %1 is %2"
                              ", size of provided list is %3");
        error = error.arg(QLatin1String(sqlname)).arg(arraySize).arg(list.size());
        setLastError(QSqlError(error, QLatin1String(""), QSqlError::StatementError));
        return false;
    }

    // [Karelin]
    QVariant::Type varType = qFirebirdTypeName(desc.array_desc_dtype,
                                               sqlVar.sqlscale < 0);
    if (!createArrayBuffer((char*)ba.constData(), list, varType, 0, &desc, error,
                           _drv->_textCodec))
    {
        setLastError(QSqlError(error.arg(QLatin1String(sqlname)), QLatin1String(""),
                     QSqlError::StatementError));
        return false;
    }

    /* readjust the buffer size*/
    if (desc.array_desc_dtype == blr_varying
        || desc.array_desc_dtype == blr_varying2)
    {
        desc.array_desc_length -= 2;
    }

    // [Karelin]
    // isc_array_put_slice(status,& ibase,& trans, arrayId,& desc, ba.data(),& bufLen);
    break_point
    isc_array_put_slice(status, _drv->ibase(), transact(), arrayId, &desc, (char*)ba.constData(), &bufLen);
    if (CHECK_ERROR("Failed call isc_array_put_slice()", QSqlError::UnknownError))
        return false;

    return true;
}

bool Result::gotoNext(SqlCachedResult::ValueCache& row, int rowIdx)
{
    if (_drv->operationIsAborted())
    {
        setAt(QSql::AfterLastRow);
        return false;
    }

    ISC_STATUS stat = 0;
    ISC_STATUS status[20] = {0};

    // Stored Procedures are special - they populate our _sqlda when executing,
    // so we don't have to call isc_dsql_fetch
    if (_queryType == isc_info_sql_stmt_exec_procedure)
    {
        // the first "fetch" shall succeed, all consecutive ones will fail since
        // we only have one row to fetch for stored procedures
        if (rowIdx != 0)
            stat = 100;
    }
    else
    {
        stat = isc_dsql_fetch(status, &_stmt, FBVERSION, _sqlda);
        if (CHECK_ERROR("Could not fetch next item", QSqlError::StatementError))
            return false;
    }

    if (stat == 100)
    {
        // no more rows
        setAt(QSql::AfterLastRow);
        return false;
    }

    // [Karelin]: код проверки перенесен выше
    // if (checkError("Could not fetch next item", QSqlError::StatementError))
    //    return false;

    if (rowIdx < 0) // not interested in actual values
        return true;

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
                row[idx] = QVariant((*(float*)sqlVar.sqldata));
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
                if (sqlVar.sqlsubtype == 1 /*OCTET*/)
                {
                    if (sqlVar.sqllen == 16)
                    {
                        const QUuid& uuid =
                            QUuid::fromRfc4122(QByteArray::fromRawData(sqlVar.sqldata, 16));
                        row[idx].setValue(uuid);
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

   //--- Код от первичной реализации, оставлен в качестве примера ---
#if 0 /// ### FIXME
    static char sizeInfo[] = {isc_info_sql_records};
    char buf[64];

    //qDebug() << sizeInfo;
    if (!isActive() || !isSelect())
        return -1;

        char ct;
        short len;
        int val = 0;
//    while(val == 0) {
        isc_dsql_sql_info(_status,& _stmt, sizeof(sizeInfo), sizeInfo, sizeof(buf), buf);
//        isc_database_info(_status,& d->ibase, sizeof(sizeInfo), sizeInfo, sizeof(buf), buf);

        for(int i = 0; i < 66; ++i)
            qDebug() << QString::number(buf[i]);

        for (char* c = buf + 3;* c != isc_info_end; /*nothing*/) {
            ct =* (c++);
            len = isc_vax_integer(c, 2);
            c += 2;
            val = isc_vax_integer(c, len);
            c += len;
            qDebug() << "size" << val;
            if (ct == isc_info_req_select_count)
                return val;
        }
        //qDebug() << "size -1";
        return -1;

        unsigned int i, result_size;
        if (buf[0] == isc_info_sql_records) {
            i = 3;
            result_size = isc_vax_integer(&buf[1],2);
            while (buf[i] != isc_info_end& & i < result_size) {
                len = (short)isc_vax_integer(&buf[i+1],2);
                if (buf[i] == isc_info_req_select_count)
                     return (isc_vax_integer(&buf[i+3],len));
                i += len+3;
           }
        }
//    }
    return -1;
#endif
}

int Result::size2() const
{
    if (!isSelectSql() || _preparedQuery.isEmpty())
    {
        log_error_m << "Size of result unavailable"
                    << ". Detail: Sql-statement not SELETC or not prepared";
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

    for (char* pcBuf = acBuffer + 3; *pcBuf != isc_info_end; /*nothing*/)
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
}

QSqlRecord Result::record() const
{
    QSqlRecord rec;
    if (!isActive() || !_sqlda)
        return rec;

    for (int i = 0; i < _sqlda->sqld; ++i)
    {
        const XSQLVAR& v = _sqlda->sqlvar[i];
        QVariant::Type fieldType = qFirebirdTypeName2(v.sqltype, v.sqlscale < 0, v.sqlsubtype, v.sqllen);
        QSqlField f {QString::fromLatin1(v.aliasname, v.aliasname_length).simplified(), fieldType};
        f.setLength(v.sqllen);
        f.setPrecision(qAbs(v.sqlscale));
        f.setRequiredStatus((v.sqltype & 1) == 0 ? QSqlField::Required : QSqlField::Optional);
        if (v.sqlscale < 0)
        {
            // В эту точку попадаем при использовании типа Numeric(x,y) в БД.
            // Необходимо более подробно исследовать то, какие выполняются
            // преобразования при работе с этим типом.
            break_point

            QSqlQuery q {new Result(_drv, Result::ForwardOnly::Yes)};
            q.setForwardOnly(true);
            q.exec(QLatin1String("select b.RDB$FIELD_PRECISION, b.RDB$FIELD_SCALE, b.RDB$FIELD_LENGTH, a.RDB$NULL_FLAG "
                   "FROM RDB$RELATION_FIELDS a, RDB$FIELDS b "
                   "WHERE b.RDB$FIELD_NAME = a.RDB$FIELD_SOURCE "
                   "AND a.RDB$RELATION_NAME = '") + QString::fromLatin1(v.relname, v.relname_length).toUpper() + QLatin1String("' "
                   "AND a.RDB$FIELD_NAME = '") + QString::fromLatin1(v.sqlname, v.sqlname_length).toUpper() + QLatin1String("' "));

            if (q.first())
            {
                if (v.sqlscale < 0)
                {
                    f.setLength(q.value(0).toInt());
                    f.setPrecision(qAbs(q.value(1).toInt()));
                }
                else
                {
                    f.setLength(q.value(2).toInt());
                    f.setPrecision(0);
                }
                f.setRequiredStatus(q.value(3).toBool() ? QSqlField::Required : QSqlField::Optional);
            }
        }
        f.setSqlType(v.sqltype);
        rec.append(f);
    }
    return rec;
}

QVariant Result::handle() const
{
    return QVariant(qRegisterMetaType<isc_stmt_handle>("isc_stmt_handle"), &_stmt);
}

//-------------------------------- Driver ------------------------------------

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

bool Driver::open(const QString& db,
                  const QString& user,
                  const QString& password,
                  const QString& host,
                  int   port,
                  const QString& connOpts)
{
    if (isOpen())
        close();

    const QStringList opts {connOpts.split(QLatin1Char(';'), QString::SkipEmptyParts)};

    QString encString;
    QByteArray role;
    for (int i = 0; i < opts.count(); ++i)
    {
        QString tmp(opts.at(i).simplified());
        int idx;
        if ((idx = tmp.indexOf(QLatin1Char('='))) != -1)
        {
            QString val = tmp.mid(idx + 1).simplified();
            QString opt = tmp.left(idx).simplified();
            if (opt.toUpper() == QLatin1String("ISC_DPB_LC_CTYPE"))
            {
                encString = val;
            }
            else if (opt.toUpper() == QLatin1String("ISC_DPB_SQL_ROLE_NAME"))
            {
                role = val.toLocal8Bit();
                role.truncate(255);
            }
        }
    }

    // Use UNICODE_FSS when no ISC_DPB_LC_CTYPE is provided
    if (encString.isEmpty())
    {
        //encString = QLatin1String("UNICODE_FSS");
        encString = QLatin1String("UTF-8");
    }
    else
    {
        _textCodec = QTextCodec::codecForName(encString.toLocal8Bit());
        if (!_textCodec)
        {
            log_warn_m << "Unsupported encoding: " << encString.toLocal8Bit()
                       << ". Using UNICODE_FFS for ISC_DPB_LC_CTYPE";
            encString = QLatin1String("UNICODE_FSS"); // Fallback to UNICODE_FSS
        }
    }

    QByteArray enc = encString.toLocal8Bit();
    QByteArray usr = user.toLocal8Bit();
    QByteArray pass = password.toLocal8Bit();
    enc.truncate(255);
    usr.truncate(255);
    pass.truncate(255);

    // [Karelin] 
    QByteArray ba;
    {
        QDataStream s(&ba, QIODevice::WriteOnly);
        s << uchar(isc_dpb_version1);
        s << uchar(isc_dpb_user_name);
        s << uchar(usr.length());
        s.writeRawData(usr.constData(), usr.length());
        s << uchar(isc_dpb_password);
        s << uchar(pass.length());
        s.writeRawData(pass.constData(), pass.length());
        s << uchar(isc_dpb_lc_ctype);
        s << uchar(enc.length());
        s.writeRawData(enc.constData(), enc.length());

        if (!role.isEmpty())
        {
            s << uchar(isc_dpb_sql_role_name);
            s << uchar(role.length());
            s.writeRawData(role.constData(), role.length());
        }
    }

    QString portString;
    if (port != -1)
        portString = QString("/%1").arg(port);

    QString ldb;
    if (!host.isEmpty())
        ldb += host + portString + QLatin1Char(':');
    ldb += db;

    log_verbose_m << "Try open database"
                  << ". User: " << user
                  << ", path: " << db
                  << ", host: " << host
                  << ", port: " << port;

    _ibase = 0;
    ISC_STATUS status[20] = {0};
    isc_attach_database(status, 0,
                        // [Karelin]
                        // const_cast<char* >(ldb.toLocal8Bit().constData()),
                        // &d->ibase, i, ba.data());
                        ldb.toLocal8Bit().constData(),
                        &_ibase, ba.length(), (char*)ba.constData());

    if (CHECK_ERROR("Error opening database", QSqlError::ConnectionError))
    {
        _ibase = 0;
        setOpenError(true);
        return false;
    }
    setOpen(true);
    log_verbose_m << "Database is open. Connect: " << _ibase;

    // Установка флага отмены операции
    memset(status, 0, sizeof(status));
    fb_cancel_operation(status, &_ibase, fb_cancel_enable);
    if (CHECK_ERROR("Failed set abort-operation flag", QSqlError::UnknownError))
    {
        close();
        return false;
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

    if (_eventBuffers.size())
    {
        // Отладить
        break_point

        ISC_STATUS status[20];
        //QMap<QString, QFirebirdEventBuffer*>::const_iterator i;
        //for (i = d->eventBuffers.constBegin(); i != d->eventBuffers.constEnd(); ++i)
        for (EventBuffer* eBuffer : _eventBuffers)
        {
            //EventBuffer* eBuffer = it->value();
            eBuffer->subscriptState = EventBuffer::Finished;
            isc_cancel_events(status, &_ibase, &eBuffer->eventId);
            //qFreeEventBuffer(eBuffer);
        }
        _eventBuffers.clear();

#if defined(FB_API_VER)
        // Workaround for Firebird crash
        QTime timer;
        timer.start();
        while (timer.elapsed() < 500)
            QCoreApplication::processEvents();
#endif
    }

    isc_db_handle ibase = _ibase;
    ISC_STATUS status[20] = {0};
    isc_detach_database(status, &_ibase);

    QString msg = QString("Error close database. Connect: %1").arg(ibase);
    CHECK_ERROR(msg.toUtf8().constData(), QSqlError::UnknownError);

    _ibase = 0;
    setOpen(false);
    setOpenError(false);
    log_verbose_m << "Database is closed. Connect: " << ibase;
}

bool Driver::isOpen() const
{
    return _isOpen;
}

void Driver::setOpen(bool val)
{
    QSqlDriver::setOpen(val);
    _isOpen = val;
}

Transaction::Ptr Driver::createTransact() const
{
    return Transaction::Ptr(new Transaction(Driver::Ptr((Driver*)this)));
}

QSqlResult* Driver::createResult() const
{
    return new Result(Driver::Ptr((Driver*)this), Result::ForwardOnly::Yes);
}

QSqlResult* Driver::createResult(const Transaction::Ptr& transact) const
{
    return new Result(transact, Result::ForwardOnly::Yes);
}

QVariant Driver::handle() const
{
    return QVariant(qRegisterMetaType<isc_db_handle>("isc_db_handle"), &_ibase);
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

QStringList Driver::tables(QSql::TableType type) const
{
    QStringList res;
    if (!isOpen())
        return res;

    QString typeFilter;
    if (type == QSql::SystemTables)
    {
        typeFilter += " AND RDB$SYSTEM_FLAG != 0 ";
    }
    else if (type == (QSql::SystemTables | QSql::Views))
    {
        typeFilter += " AND (RDB$SYSTEM_FLAG != 0 OR RDB$VIEW_BLR NOT NULL) ";
    }
    else
    {
        if (!(type & QSql::SystemTables))
            typeFilter += " AND RDB$SYSTEM_FLAG = 0 ";

        if (!(type & QSql::Views))
            typeFilter += " AND RDB$VIEW_BLR IS NULL ";

        if (!(type & QSql::Tables))
            typeFilter += " AND RDB$VIEW_BLR IS NOT NULL ";

        //if (!typeFilter.isEmpty())
        //    typeFilter.chop(5);
    }
    //if (!typeFilter.isEmpty())
    //    typeFilter.prepend(QLatin1String("where "));

    break_point
    // Проверить работу

    QSqlQuery q {createResult()};
    q.setForwardOnly(true);

    //if (!q.exec(QLatin1String("select rdb$relation_name from rdb$relations ") + typeFilter))
    //    return res;

    if (!q.prepare(" SELECT RDB$RELATION_NAME FROM RDB$RELATIONS "
                   " WHERE (1 = 1) " + typeFilter))
    {
        return res;
    }
    if (!q.exec())
        return res;

    while (q.next())
        res << q.value(0).toString().simplified();

    return res;
}

QSqlRecord Driver::record(const QString& tableName) const
{
    QSqlRecord rec;
    if (!isOpen())
        return rec;

    QString table = tableName;
    if (isIdentifierEscaped(table, QSqlDriver::TableName))
        table = stripDelimiters(table, QSqlDriver::TableName);
    else
        table = table.toUpper();

    break_point
    // Проверить работу

    QSqlQuery q {createResult()};
    q.setForwardOnly(true);

    if (!q.prepare(
        " SELECT                                      "
        "   A.RDB$FIELD_NAME,                         "
        "   B.RDB$FIELD_TYPE,                         "
        "   B.RDB$FIELD_LENGTH,                       "
        "   B.RDB$FIELD_SCALE,                        "
        "   B.RDB$FIELD_PRECISION,                    "
        "   A.RDB$NULL_FLAG                           "
        " FROM RDB$RELATION_FIELDS A, RDB$FIELDS B    "
        " WHERE B.RDB$FIELD_NAME = A.RDB$FIELD_SOURCE "
        "   AND A.RDB$RELATION_NAME = :TABLE_NAME     "
        " ORDER BY A.RDB$FIELD_POSITION               "))
    {
        return rec;
    }
    q.bindValue(":TABLE_NAME", table);

    if (!q.exec())
        return rec;

    while (q.next())
    {
        int type = q.value(1).toInt();
        bool hasScale = (q.value(3).toInt() < 0);
        QSqlField field (q.value(0).toString().simplified(),
                         qFirebirdTypeName(type, hasScale));
        if (hasScale)
        {
            field.setLength(q.value(4).toInt());
            field.setPrecision(qAbs(q.value(3).toInt()));
        }
        else
        {
            field.setLength(q.value(2).toInt());
            field.setPrecision(0);
        }
        field.setRequired(q.value(5).toInt() > 0 ? true : false);
        field.setSqlType(type);

        rec.append(field);
    }
    return rec;
}

QSqlIndex Driver::primaryIndex(const QString& tableName) const
{
    // Отладить
    break_point

    QSqlIndex index {tableName};
    if (!isOpen())
        return index;

    QString table = tableName;
    if (isIdentifierEscaped(table, QSqlDriver::TableName))
        table = stripDelimiters(table, QSqlDriver::TableName);
    else
        table = table.toUpper();

    QSqlQuery q {createResult()};
    q.setForwardOnly(true);

    if (!q.prepare(
        " SELECT                                          "
        "   A.RDB$INDEX_NAME,                             "
        "   B.RDB$FIELD_NAME,                             "
        "   D.RDB$FIELD_TYPE,                             "
        "   D.RDB$FIELD_SCALE                             "
        " FROM RDB$RELATION_CONSTRAINTS A,                "
        "   RDB$INDEX_SEGMENTS B,                         "
        "   RDB$RELATION_FIELDS C,                        "
        "   RDB$FIELDS D                                  "
        " WHERE A.RDB$CONSTRAINT_TYPE = 'PRIMARY KEY'     "
        "   AND A.RDB$RELATION_NAME = :TABLE_NAME         "
        "   AND A.RDB$INDEX_NAME = B.RDB$INDEX_NAME       "
        "   AND C.RDB$RELATION_NAME = A.RDB$RELATION_NAME "
        "   AND C.RDB$FIELD_NAME = B.RDB$FIELD_NAME       "
        "   AND D.RDB$FIELD_NAME = C.RDB$FIELD_SOURCE     "
        "ORDER BY B.RDB$FIELD_POSITION                    "))
    {
        return index;
    }
    q.bindValue(":TABLE_NAME", table);

    if (!q.exec())
        return index;

    while (q.next())
    {
        int type = q.value(2).toInt();
        bool hasScale = (q.value(3).toInt() < 0);
        QSqlField field (q.value(1).toString().simplified(),
                         qFirebirdTypeName(type, hasScale));
        index.append(field); //TODO: asc? desc?
        index.setName(q.value(0).toString());
    }
    return index;
}

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wswitch-enum"

QString Driver::formatValue(const QSqlField& field, bool trimStrings) const
{
    switch (field.type())
    {
        case QVariant::DateTime:
        {
            QDateTime datetime = field.value().toDateTime();
            if (datetime.isValid())
            {
                return QLatin1Char('\'')
                       + QString::number(datetime.date().year())   + QLatin1Char('-')
                       + QString::number(datetime.date().month())  + QLatin1Char('-')
                       + QString::number(datetime.date().day())    + QLatin1Char(' ')
                       + QString::number(datetime.time().hour())   + QLatin1Char(':')
                       + QString::number(datetime.time().minute()) + QLatin1Char(':')
                       + QString::number(datetime.time().second()) + QLatin1Char('.')
                       + QString::number(datetime.time().msec()).rightJustified(3, QLatin1Char('0'), true)
                       + QLatin1Char('\'');
            }
            else
                return QLatin1String("NULL");
        }
        case QVariant::Time:
        {
            QTime time = field.value().toTime();
            if (time.isValid())
            {
                return QLatin1Char('\'')
                       + QString::number(time.hour())   + QLatin1Char(':')
                       + QString::number(time.minute()) + QLatin1Char(':')
                       + QString::number(time.second()) + QLatin1Char('.')
                       + QString::number(time.msec()).rightJustified(3, QLatin1Char('0'), true)
                       + QLatin1Char('\'');
            }
            else
                return QLatin1String("NULL");
        }
        case QVariant::Date:
        {
            QDate date = field.value().toDate();
            if (date.isValid())
            {
                return QLatin1Char('\'')
                       + QString::number(date.year())  + QLatin1Char('-')
                       + QString::number(date.month()) + QLatin1Char('-')
                       + QString::number(date.day())
                       + QLatin1Char('\'');
            }
            else
                return QLatin1String("NULL");
        }
        default:
            return QSqlDriver::formatValue(field, trimStrings);
    }
}

#pragma GCC diagnostic pop

namespace {
void qEventCallback(void* result, ISC_USHORT length, const ISC_UCHAR* updated)
{
    if (!updated)
        return;

    // Отладить
    break_point

    memcpy(result, updated, length);
    //qMutex()->lock();
    //Driver* driver = qBufferDriverMap()->value(result);
    //qMutex()->unlock();

    // We use an asynchronous call (i.e., queued connection) because the event callback
    // is executed in a different thread than the one in which the driver lives.
    //if (driver)
    //    QMetaObject::invokeMethod(driver, "qHandleEventNotification", Qt::QueuedConnection,
    //                              Q_ARG(void* , reinterpret_cast<void*>(result)));

    //qHandleEventNotification(reinterpret_cast<void*>(result));
}
} // namespace

#if QT_VERSION >= 0x050000
bool Driver::subscribeToNotification(const QString& name)
{
    return subscribeToNotificationImplementation(name);
}

bool Driver::unsubscribeFromNotification(const QString& name)
{
    return unsubscribeFromNotificationImplementation(name);
}

QStringList Driver::subscribedToNotifications() const
{
    return subscribedToNotificationsImplementation();
}
#endif

bool Driver::subscribeToNotificationImplementation(const QString& name)
{
    // Отладить
    break_point

    if (!isOpen())
    {
        log_warn_m << "subscribeToNotification(): database not open";
        return false;
    }
    if (_eventBuffers.contains(name))
    {
        log_warn_m << "subscribeToNotification(): already subscribing to " << name;
        return false;
    }

    EventBuffer* eBuffer = new EventBuffer;
    eBuffer->subscriptState = EventBuffer::Starting;
    eBuffer->bufferLength = isc_event_block(&eBuffer->eventBuffer,
                                            &eBuffer->resultBuffer,
                                            1,
                                            name.toLocal8Bit().constData());

//    qMutex()->lock();
//    qBufferDriverMap()->insert(eBuffer->resultBuffer, this);
//    qMutex()->unlock();

    _eventBuffers.insert(name, eBuffer);

    ISC_STATUS status[20];
    isc_que_events(status,
                   &_ibase,
                   &eBuffer->eventId,
                   eBuffer->bufferLength,
                   eBuffer->eventBuffer,
                   (ISC_EVENT_CALLBACK)qEventCallback,
                   eBuffer->resultBuffer);

    if (status[0] == 1 && status[1])
    {
        QString msg = QString::fromLatin1("Could not subscribe to event notifications for %1").arg(name);
        log_error_m << "subscribeToNotification(): " << msg;
        setLastError(QSqlError(msg));
        _eventBuffers.remove(name);
        //qFreeEventBuffer(eBuffer);
        return false;
    }
    return true;
}

bool Driver::unsubscribeFromNotificationImplementation(const QString& name)
{
    // Отладить
    break_point

    if (!isOpen())
    {
        log_warn_m << "unsubscribeFromNotification(): database not open";
        return false;
    }
    if (!_eventBuffers.contains(name))
    {
        log_warn_m << "unsubscribeFromNotification(): not subscribed to " << name;
        return false;
    }

    EventBuffer* eBuffer = _eventBuffers.value(name);
    ISC_STATUS status[20];
    eBuffer->subscriptState = EventBuffer::Finished;
    isc_cancel_events(status, &_ibase, &eBuffer->eventId);

    if (status[0] == 1 && status[1])
    {
        QString msg = QString::fromLatin1("Could not unsubscribe from event notifications for %1").arg(name);
        log_error_m << "unsubscribeFromNotification(): " << msg;
        setLastError(QSqlError(msg));
        return false;
    }

    _eventBuffers.remove(name);
    //qFreeEventBuffer(eBuffer);
    return true;
}

QStringList Driver::subscribedToNotificationsImplementation() const
{
    // Отладить
    break_point

    return QStringList(_eventBuffers.keys());
}

void Driver::qHandleEventNotification(void* updatedResultBuffer)
{
    // Отладить
    break_point

    //QMap<QString, QFirebirdEventBuffer*>::const_iterator i;
    //for (i = d->eventBuffers.constBegin(); i != d->eventBuffers.constEnd(); ++i)
    for (auto it = _eventBuffers.constBegin(); it != _eventBuffers.constEnd(); ++it)
    {
        EventBuffer* eBuffer = it.value();
        if (reinterpret_cast<void*>(eBuffer->resultBuffer) != updatedResultBuffer)
            continue;

        ISC_ULONG counts[20];
        memset(counts, 0, sizeof(counts));
        isc_event_counts(counts, eBuffer->bufferLength, eBuffer->eventBuffer, eBuffer->resultBuffer);
        if (counts[0])
        {
            if (eBuffer->subscriptState == EventBuffer::Subscribed)
            {
                break_point
                emit notification(it.key());
            }
            else if (eBuffer->subscriptState == EventBuffer::Starting)
            {
                eBuffer->subscriptState = EventBuffer::Subscribed;
            }
            ISC_STATUS status[20];
            isc_que_events(status,
                           &_ibase,
                           &eBuffer->eventId,
                           eBuffer->bufferLength,
                           eBuffer->eventBuffer,
                           (ISC_EVENT_CALLBACK)qEventCallback,
                           eBuffer->resultBuffer);
            if (Q_UNLIKELY(status[0] == 1 && status[1]))
            {
                log_error_m << "qHandleEventNotification(): could not resubscribe to " << it.key();
            }
            return;
        }
    }
}

bool Driver::checkError(const char* msg, QSqlError::ErrorType type,
                        ISC_STATUS* status, const char* func, int line)
{
    ISC_LONG sqlcode; QString err;
    if (firebirdError(status, _textCodec, sqlcode, err))
    {
        setLastError(QSqlError("FirebirdDriver", msg, type, 1));
        alog::logger().error(alog::detail::file_name(__FILE__), func, line, "FirebirdDrv")
            << msg << ". Detail: " << err << "; SqlCode: " << sqlcode;
        return true;
    }
    return false;
}

QString Driver::escapeIdentifier(const QString& identifier, IdentifierType) const
{
    QString res = identifier;
    if (!identifier.isEmpty()
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
                  << ". Connect: " << _ibase
                  << " (call from thread: " << trd::gettid() << ")";

    _operationIsAborted = true;

    ISC_STATUS status[20] = {0};
    //fb_cancel_operation(status, &_ibase, fb_cancel_raise);
    fb_cancel_operation(status, &_ibase, fb_cancel_abort);
    CHECK_ERROR("Failed abort sql-operation", QSqlError::UnknownError);
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

static int ignoreSIGTERM(const int reason, const int, void*)
{
    return reason == fb_shutrsn_signal ? 1 : 0;
}

bool setIgnoreSIGTERM()
{
    ISC_STATUS_ARRAY status = {0};
    if (fb_shutdown_callback(status, ignoreSIGTERM, fb_shut_confirmation, 0))
    {
        ISC_LONG sqlcode; QString err;
        if (firebirdError(status, 0, sqlcode, err))
            log_error_m << err << "; SqlCode: " << sqlcode;
        else
            log_error_m << "Unknown error";
        return false;
    }
    return true;
}

int resultSize(const QSqlQuery& q)
{
    if (const Result* r = dynamic_cast<const Result*>(q.result()))
        return r->size2();

    return -1;
}

#undef CHECK_ERROR
#undef SET_LAST_ERROR

} // namespace firebird
} // namespace db
