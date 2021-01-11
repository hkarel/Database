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

#include "firebird_func.h"

#include "shared/logger/logger.h"
#include "shared/logger/format.h"
#include "shared/config/appl_conf.h"
#include "shared/qt/logger_operators.h"

#include <QtCore>

namespace db {
namespace firebird {

bool asyncCheck(const YamlConfig& conf)
{
    QString databaseFile;
    int databaseAsyncDetect = -1;
    {
        QString gstat = "/opt/firebird/bin/gstat";
        if (!QFile::exists(gstat))
        {
            log_error << "Utility gstat not found: " << gstat;
            return false;
        }

#ifndef MINGW
        conf.getValue("database.file", databaseFile);
#else
        conf.getValue("database.file_win", databaseFile);
#endif
        config::dirExpansion(databaseFile);

        if (!QFile::exists(databaseFile))
        {
            log_error << "Database file not found: " << databaseFile;
            return false;
        }

        QProcess proc;
        QStringList arguments;

        arguments << "-h" << databaseFile;
        proc.start(gstat, arguments, QIODevice::ReadOnly);

        if (!proc.waitForStarted(5000))
        {
            proc.kill();
            log_error << "Failed run utility gstat: " << gstat
                      << ". Run timeout expired";
            return false;
        }

        while (proc.state() == QProcess::Running)
        {
            proc.waitForReadyRead(1000);

            QList<QByteArray> split = proc.readAllStandardOutput().split('\n');
            for (const QByteArray& s : split)
            {
                if (s.contains("Attributes"))
                {
                    databaseAsyncDetect = 1;
                    if (s.contains("force write"))
                        databaseAsyncDetect = 0;

                    proc.closeReadChannel(QProcess::StandardOutput);
                    proc.closeReadChannel(QProcess::StandardError);
                    break;
                }
            }
            if (databaseAsyncDetect != -1)
            {
                if (!proc.waitForFinished(5*1000 /*5 сек*/))
                    log_error << "Waiting finished gstat (timeout expired)";
                else
                    break;
            }
        }
    }
    if (databaseAsyncDetect == -1)
    {
        log_error << "Failed detect database sync/async mode";
        return false;
    }
    if (databaseAsyncDetect == 0)
    {
        log_error << log_format(
            "Database work in 'sync' mode. Asynchronous mode required"
            ". Run the command: 'sudo /opt/firebird/bin/gfix -write async %?'", databaseFile);
        return false;
    }

    return true;
}

} // namespace firebird
} // namespace db
