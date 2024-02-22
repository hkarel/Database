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

#include <QRegularExpression>

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

void assignValue(qint8& val, const QSqlRecord& rec, const QString& fieldName)
{
    const QSqlField& f = rec.field(fieldName.trimmed());
    if (f.isNull() || !f.isValid())
        return;

    if (f.value().canConvert<int>())
        val = qint8(f.value().value<int>());
}

void assignValue(quint8& val, const QSqlRecord& rec, const QString& fieldName)
{
    const QSqlField& f = rec.field(fieldName.trimmed());
    if (f.isNull() || !f.isValid())
        return;

    if (f.value().canConvert<int>())
    {
        qint8 v = qint8(f.value().value<int>());
        val = *((quint8*) &v);
    }
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

void assignValue(float& val, const QSqlRecord& rec, const QString& fieldName)
{
    const QSqlField& f = rec.field(fieldName.trimmed());
    if (f.isNull() || !f.isValid())
        return;

    if (f.value().canConvert<float>())
        val = f.value().value<float>();
}

QString fieldsToPlaceholders(QString fields)
{
    static QRegularExpression reg {R"(\s+)"};
    fields.remove(reg);
    fields.replace(",", ", :");
    fields.prepend(":");
    return fields;
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

QString insertOrUpdateStatementPG(const QString& tableName, const QString& fields,
                                  const QString& matching)
{
    QString sql = "INSERT INTO %1 (%2) VALUES (%3) ON CONFLICT (%4) DO UPDATE SET %5";
    QString placeholders = fieldsToPlaceholders(fields);
    QStringList list = fields.split(',');
    for (QString& s : list)
    {
        s = s.trimmed();
        s = s + "=:" + s;
    }
    QString updateFields = list.join(',');
    sql = sql.arg(tableName)
             .arg(fields)
             .arg(placeholders)
             .arg(matching)
             .arg(updateFields);

    return sql;
}

QString mergeRowStatementMS(const QString& tableName,
                            const QList<QString>& fields,
                            const QList<QString>& matching)
{
    QString sql =
        " MERGE %1 AS Target                  "
        " USING (SELECT %2) AS Source         "
        "    ON (%3)                          "
        " WHEN MATCHED THEN UPDATE SET        "
        "   %4                                "
        " WHEN NOT MATCHED THEN INSERT VALUES "
        " (                                   "
        "   %5                                "
        " );                                  ";

    //QString rowValues;

    // :F_GUID as F_GUID
    QStringList valuesHolders;
    for(const QString& field : fields)
    {
        QString holder = field;
        holder = holder.replace('[', ' ');// TODO: remove
        holder = holder.replace(']', ' ');
        holder = holder.trimmed();
        QString finalHolder = ":%1 as %2";
        finalHolder = finalHolder.arg(holder).arg(field);
        valuesHolders.append(finalHolder);
    }
    QString valueFields = valuesHolders.join(',');

    QStringList matchHolders;
    for(const QString& match : matching)
    {
        QString holder = "Target.%1 = Source.%2";
        holder = holder.arg(match).arg(match);
        matchHolders.append(holder);
    }
    QString matchFields = matchHolders.join(',');

    QStringList matchedHolders;
    for(const QString& field : fields)
    {
        QString holder = "%1 = Source.%2";
        holder = holder.arg(field).arg(field);
        matchedHolders.append(holder);
    }
    QString matchedFields = matchedHolders.join(',');

    QStringList notMatchedHolders;
    for(const QString& field : fields)
    {
        QString holder = "Source.%1";
        holder = holder.arg(field);
        notMatchedHolders.append(holder);
    }
    QString notMatchedFields = notMatchedHolders.join(',');

    sql = sql.arg(tableName)
             .arg(valueFields)
             .arg(matchFields)
             .arg(matchedFields)
             .arg(notMatchedFields);

    return sql;
}

QString mergeRowStatementMS(const QString& tableName,
                            const QString& fields,
                            const QString& matching)
{
    return mergeRowStatementMS(tableName, fields.split(','), matching.split(','));
}

QString mergeTableStatementMS(const QString& targetTableName,
                              const QString& sourceTableName,
                              const QList<QString>& fields,
                              const QList<QString>& matching)
{
    QString sql =
        " MERGE %1 AS Target                  "
        " USING %2 AS Source                  "
        "    ON (%3)                          "
        " WHEN MATCHED THEN UPDATE SET        "
        "   %4                                "
        " WHEN NOT MATCHED THEN INSERT VALUES "
        " (                                   "
        "   %5                                "
        " );                                  ";

    QStringList matchHolders;
    for(const QString& match: matching)
    {
        QString holder = "Target.%1 = Source.%2";
        holder = holder.arg(match).arg(match);
        matchHolders.append(holder);
    }
    QString matchFields = matchHolders.join(',');

    QStringList matchedHolders;
    for (const QString& field : fields)
    {
        QString holder = "%1 = Source.%2";
        holder = holder.arg(field).arg(field);
        matchedHolders.append(holder);
    }
    QString matchedFields = matchedHolders.join(',');

    QStringList notMatchedHolders;
    for (const QString& field : fields)
    {
        QString holder = "Source.%1";
        holder = holder.arg(field);
        notMatchedHolders.append(holder);
    }
    QString notMatchedFields = notMatchedHolders.join(',');

    sql = sql.arg(targetTableName)
             .arg(sourceTableName)
             .arg(matchFields)
             .arg(matchedFields)
             .arg(notMatchedFields);

    return sql;
}

QString mergeTableStatementMS(const QString& targetTableName,
                              const QString& sourceTableName,
                              const QString& fields,
                              const QString& matching)
{
    return mergeTableStatementMS(targetTableName, sourceTableName,
                                 fields.split(','), matching.split(','));
}

} // namespace sql
