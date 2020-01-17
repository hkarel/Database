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

#include "sql_func.h"

namespace sql {

QVariant bindVariant(bool val)
{
    return QVariant(int(val));
}

QVariant bindVariant(qint16 val)
{
    return QVariant(int(val));
}

QVariant bindVariant(quint16 val)
{
    qint16 v = *((qint16*) &val);
    return QVariant(int(v));
}

QVariant bindVariant(qint32 val)
{
    return QVariant(val);
}

QVariant bindVariant(quint32 val)
{
    qint32 v = *((qint32*) &val);
    return QVariant(v);
}

QVariant bindVariant(qint64 val)
{
    return QVariant(val);
}

QVariant bindVariant(quint64 val)
{
    qint64 v = *((qint64*) &val);
    return QVariant(v);
}

QVariant bindVariant(const char* val)
{
    return QVariant(QString::fromLatin1(val));
}

void assignValue(bool& val, const QSqlRecord& rec, const QString& fieldName)
{
    const QSqlField& f = rec.field(fieldName.trimmed());
    if (f.isNull() || !f.isValid())
        return;

    if (f.value().canConvert<int>())
        val = f.value().value<int>();
}

void assignValue(qint16& val, const QSqlRecord& rec, const QString& fieldName)
{
    const QSqlField& f = rec.field(fieldName.trimmed());
    if (f.isNull() || !f.isValid())
        return;

    if (f.value().canConvert<int>())
        val = qint16(f.value().value<int>());
}

void assignValue(quint16& val, const QSqlRecord& rec, const QString& fieldName)
{
    const QSqlField& f = rec.field(fieldName.trimmed());
    if (f.isNull() || !f.isValid())
        return;

    if (f.value().canConvert<int>())
    {
        qint16 v = qint16(f.value().value<int>());
        val = *((quint16*) &v);
    }
}

void assignValue(qint32& val, const QSqlRecord& rec, const QString& fieldName)
{
    const QSqlField& f = rec.field(fieldName.trimmed());
    if (f.isNull() || !f.isValid())
        return;

    if (f.value().canConvert<qint32>())
        val = f.value().value<qint32>();
}

void assignValue(quint32& val, const QSqlRecord& rec, const QString& fieldName)
{
    const QSqlField& f = rec.field(fieldName.trimmed());
    if (f.isNull() || !f.isValid())
        return;

    if (f.value().canConvert<qint32>())
    {
        qint32 v = f.value().value<qint32>();
        val = *((quint32*) &v);
    }
}

void assignValue(qint64& val, const QSqlRecord& rec, const QString& fieldName)
{
    const QSqlField& f = rec.field(fieldName.trimmed());
    if (f.isNull() || !f.isValid())
        return;

    if (f.value().canConvert<qint64>())
        val = f.value().value<qint64>();
}

void assignValue(quint64& val, const QSqlRecord& rec, const QString& fieldName)
{
    const QSqlField& f = rec.field(fieldName.trimmed());
    if (f.isNull() || !f.isValid())
        return;

    if (f.value().canConvert<qint64>())
    {
        qint64 v = f.value().value<qint64>();
        val = *((quint64*) &v);
    }
}

QString fieldsToPlaceholders(QString fields)
{
    static QRegExp reg {R"(\s+)"};
    fields.remove(reg);
    fields.replace(",", ", :");
    fields.prepend(":");
    return fields;
}

QString fieldsToPlaceholders(const QList<QString>& fields)
{
    static QRegExp reg {R"(\s+)"};
    QList<QString> copy = fields;
    QString line;

    line = ":" + copy.first();
    copy.removeFirst();

    for(const QString& item : copy)
    {
        QString s = item;
        s.remove(reg);
        s = s.prepend(", :");
        line += s;

    }


    return line;
}

QString fieldsListToString(const QList<QString>& fields)
{
    QList<QString> copy = fields;
    QString list = copy.first();
    copy.removeFirst();

    for(const QString& item : copy)
    {
        list += ", " + item;
    }
    return list;
}

QString fieldsToPlaceholders(const QList<QString>& fields, const QString& matching)
{
    QList<QString> copy = fields;
    copy.removeOne(matching);

    QString list = copy.first() + "= :" + copy.first();
    copy.removeFirst();

    for(const QString& item : copy)
    {
        list += ", " + item + " = :" + item;
    }
    return list;
}


QString insertIntoStatement(const QString& tableName, const QString& fields)
{
    QString sql = "INSERT INTO %1 (%2) VALUES (%3)";
    QString placeholders = fieldsToPlaceholders(fields);
    sql = sql.arg(tableName).arg(fields).arg(placeholders);
    return sql;
}

QString updateOrInsertStatement(const QString& tableName, const QString& fields,
                                const QString& matching)
{
    QString sql = "UPDATE OR INSERT INTO %1 (%2) VALUES (%3) MATCHING (%4)";
    QString placeholders = fieldsToPlaceholders(fields);
    sql = sql.arg(tableName).arg(fields).arg(placeholders).arg(matching);
    return sql;
}

QString insertOnConflictNothing(const QString& tableName, const QString& fields,
                                const QString& matching)
{
    QString sql = "INSERT INTO %1 (%2) VALUES (%3) ON CONFLICT (%4) DO NOTHING";
    QString placeholders = fieldsToPlaceholders(fields);
    sql = sql.arg(tableName).arg(fields).arg(placeholders).arg(matching);
    return sql;
}

QString insertOnConflictUpdate(const QString& tableName, const QList<QString>& fields,
                                const QString& matching)
{
    QString sql = "INSERT INTO %1 (%2) VALUES (%3) ON CONFLICT (%4) DO UPDATE SET %5";
    QString fieldsString = fieldsListToString(fields);
    QString placeholders = fieldsToPlaceholders(fields);
    QString placeholdersWoMatch = fieldsToPlaceholders(fields, matching);
    sql = sql.arg(tableName).arg(fieldsString).arg(placeholders).arg(matching).arg(placeholdersWoMatch);
    return sql;
}

} // namespace sql
