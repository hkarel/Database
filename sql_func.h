/*****************************************************************************
  The MIT License

  Copyright © 2019 Pavel Karelin (hkarel), <hkarel@yandex.ru>

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
#include "shared/prog_abort.h"
#include "shared/qt/quuidex.h"
#include "shared/logger/logger.h"

#include <QtSql>
#include <type_traits>

namespace sql {
namespace detail {

template<typename T>
struct not_enum_type : std::enable_if<!std::is_enum<T>::value, int> {};
template<typename T>
struct is_enum_type : std::enable_if<std::is_enum<T>::value, int> {};

//template<typename T>
//auto bindVariant(const T& val, int, int) -> decltype(QVariant(val))
//{
//    if (qMetaTypeId<T>() == qMetaTypeId<QUuidEx>()
//        || qMetaTypeId<T>() == qMetaTypeId<QUuid>())
//    {
//        return QVariant::fromValue(val);
//    }
//
//    return QVariant(val);
//}

//template<typename T>
//auto bindVariant(const T& val, long, long) -> decltype(QVariant())
//{
//    int typeId = qMetaTypeId<T>();
//    if (QMetaType::Type(typeId) >= QMetaType::User)
//        return QVariant::fromValue(val);
//
//    return QVariant();
//}

} // namespace detail

QVariant bindVariant(bool);
QVariant bindVariant(qint16);
QVariant bindVariant(quint16);
QVariant bindVariant(qint32);
QVariant bindVariant(quint32);
QVariant bindVariant(qint64);
QVariant bindVariant(quint64);
QVariant bindVariant(const char*);

inline QVariant bindVariant(const QVariant& val) {return val;}

template<typename T>
QVariant bindVariant(const T& val, typename detail::not_enum_type<T>::type = 0)
{
    //return detail::bindVariant(val, 0, 0);
    return QVariant::fromValue(val);
}

template<typename T>
QVariant bindVariant(const T& val, typename detail::is_enum_type<T>::type = 0)
{
    static_assert(std::is_same<typename std::underlying_type<T>::type, quint32>::value,
                  "Base type of enum must be 'unsigned int'");

    return bindVariant(static_cast<quint32>(val));
}

template<typename T>
QVariant bindVariant(const QVector<T>& val, typename detail::is_enum_type<T>::type = 0)
{
    static_assert(std::is_same<typename std::underlying_type<T>::type, quint32>::value,
                  "Base type of enum must be 'unsigned int'");

    QVector<qint32> arr;
    arr.resize(val.count());
    for (int i = 0; i < val.count(); ++i)
        arr[i] = static_cast<qint32>(val[i]);

    return bindVariant(arr);
}

template<typename T>
void bindValue(QSqlQuery& q, const QString& name, const T& value)
{
    q.bindValue(name.trimmed(), bindVariant(value));
}

inline void addBindValue(QSqlQuery&) {}

template<typename T, typename... Args>
void addBindValue(QSqlQuery& q, const T& t, const Args&... args)
{
    q.addBindValue(bindVariant(t));
    addBindValue(q, args...);
}

void assignValue(bool&,    const QSqlRecord&, const QString& fieldName);
void assignValue(qint8&,   const QSqlRecord&, const QString& fieldName);
void assignValue(quint8&,  const QSqlRecord&, const QString& fieldName);
void assignValue(qint16&,  const QSqlRecord&, const QString& fieldName);
void assignValue(quint16&, const QSqlRecord&, const QString& fieldName);
void assignValue(qint32&,  const QSqlRecord&, const QString& fieldName);
void assignValue(quint32&, const QSqlRecord&, const QString& fieldName);
void assignValue(qint64&,  const QSqlRecord&, const QString& fieldName);
void assignValue(quint64&, const QSqlRecord&, const QString& fieldName);
void assignValue(float&,   const QSqlRecord&, const QString& fieldName);

template<typename T>
void assignValue(T& val, const QSqlRecord& rec, const QString& fieldName,
                 typename detail::not_enum_type<T>::type = 0)
{
    const QSqlField& f = rec.field(fieldName.trimmed());

    if (f.isNull())
        return;

    if (!f.isValid())
        return;

    int typeId = f.value().userType();
    int typeId2 = qMetaTypeId<T>();
    if (typeId != typeId2)
    {
        log_error << "Unable convert QVariant type " << typeId
                  << " to value type " << typeId2
                  << " for field " << f.name().toUtf8().constData();
        prog_abort();
    }
    val = f.value().value<T>();
}

template<typename T>
void assignValue(T& val, const QSqlRecord& rec, const QString& fieldName,
                 typename detail::is_enum_type<T>::type = 0)
{
    static_assert(std::is_same<typename std::underlying_type<T>::type, quint32>::value,
                  "Base type of enum must be 'unsigned int'");

    const QSqlField& field = rec.field(fieldName.trimmed());
    if (field.isNull() || !field.isValid())
        return;

    if (field.value().canConvert<qint32>())
    {
        qint32 v = field.value().value<qint32>();
        quint32 v2 = *((quint32*) &v);
        val = static_cast<T>(v2);
    }
}

template<int N>
void assignValue(QUuidT<N>& val, const QSqlRecord& rec, const QString& fieldName)
{
    assignValue(static_cast<QUuid&>(val), rec, fieldName);
}

template<int N>
void assignValue(QVector<QUuidT<N>>& val, const QSqlRecord& rec, const QString& fieldName)
{
    assignValue(reinterpret_cast<QVector<QUuid>&>(val), rec, fieldName);
}

template<typename T>
void assignValue(QVector<T>& val, const QSqlRecord& rec, const QString& fieldName,
                 typename detail::is_enum_type<T>::type = 0)
{
    static_assert(std::is_same<typename std::underlying_type<T>::type, quint32>::value,
                  "Base type of enum must be 'unsigned int'");

    const QSqlField& field = rec.field(fieldName.trimmed());
    if (field.isNull() || !field.isValid())
        return;

    if (field.value().canConvert<QVector<qint32>>())
    {
        QVector<qint32> arr = field.value().value<QVector<qint32>>();
        val.resize(arr.count());

        for(int i = 0; i < arr.count(); ++i)
        {
            quint32 v = *((quint32*) &arr[i]);
            val[i] = static_cast<T>(v);
        }
    }
}

template<typename... Args>
bool exec(QSqlQuery& q, const QString& sql, const Args&... args)
{
    if (!q.prepare(sql))
        return false;

    addBindValue(q, args...);
    return q.exec();
}

// Преобразует список полей в список заменителей.
// Входящий список: "FIELD1, FIELD2, FIELD3"
// Результат: ":FIELD1, :FIELD2, :FIELD3"
QString fieldsToPlaceholders(QString fields);

// Генерирует sql-запрос вида: "INSERT INTO %1 (%2) VALUES (%3)"
QString insertIntoStatement(const QString& tableName, const QString& fields);

// Генерирует sql-запрос вида:
// "UPDATE OR INSERT INTO %1 (%2) VALUES (%3) MATCHING (%4)"
QString updateOrInsertStatement(const QString& tableName, const QString& fields,
                                const QString& matching);

// Генерирует sql-запрос вида:
// "INSERT INTO %1 (%2) VALUES (%3) ON CONFLICT (%4) DO UPDATE SET..."
QString insertOrUpdateStatementPG(const QString& tableName, const QString& fields,
                                  const QString& matching);

// Генерирует sql-запрос вида:
// MERGE types_table_target AS Target
// USING types_table_source AS Source
//    ON (Target.f_bigint = Source.f_bigint)
// WHEN MATCHED THEN UPDATE SET
//    [f_bigint] = Source.[f_bigint]
//   ,[f_binary] = Source.[f_binary]
// WHEN NOT MATCHED THEN INSERT VALUES
// (
//   Source.[f_bigint]
//   Source.[f_binary]
// );
QString mergeRowStatementMS(const QString& tableName,
                            const QList<QString>& fields,
                            const QList<QString>& matching);

QString mergeRowStatementMS(const QString& tableName,
                            const QString& fields,
                            const QString& matching);

// Генерирует sql-запрос вида:
// MERGE types_table_target AS Target
// USING types_table_source AS Source
//    ON (Target.f_bigint = Source.f_bigint)
// WHEN MATCHED THEN UPDATE SET
//    [f_bigint] = Source.[f_bigint]
//   ,[f_binary] = Source.[f_binary]
// WHEN NOT MATCHED THEN INSERT VALUES
// (
//   Source.[f_bigint]
//   Source.[f_binary]
// );
QString mergeTableStatementMS(const QString& targetTableName,
                              const QString& sourceTableName,
                              const QList<QString>& fields,
                              const QList<QString>& matching);

QString mergeTableStatementMS(const QString& targetTableName,
                              const QString& sourceTableName,
                              const QString& fields,
                              const QString& matching);

} // namespace sql

#define INSERT_INTO            insertIntoStatement
#define UPDATE_OR_INSERT_INTO  updateOrInsertStatement
#define INSERT_OR_UPDATE_PG    insertOrUpdateStatementPG
#define MERGE_ROW_MS           mergeRowStatementMS
#define MERGE_TABLE_MS         mergeTableStatementMS
