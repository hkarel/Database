#pragma once

#include "shared/qt/quuidex.h"

#include <QMetaType>
#include <QtCore>

// PG_TYPE_INT4_ARRAY
Q_DECLARE_METATYPE(QVector<qint32>)

// PG_TYPE_UUID_ARRAY
Q_DECLARE_METATYPE(QVector<QUuid>)
Q_DECLARE_METATYPE(QVector<QUuidEx>)
