#include "firebird_func.h"

#include "shared/logger/logger.h"
#include "shared/logger/format.h"
#include "shared/config/appl_conf.h"
#include "shared/qt/logger_operators.h"

#include <QtCore>

namespace db {
namespace firebird {

bool databaseAsyncCheck(const YamlConfig& conf)
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
