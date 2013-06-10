package edu.cmu.graphchi;


import java.io.File;
import java.io.FileInputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.MessageFormat;
import java.util.Date;
import java.util.logging.*;


/**
 * Wrapper for Java logging.
 * Use ChiLogger.getLogger("object-name") to get a logger object.
 */
public class ChiLogger {


    public static void init() {
        try {
            File logProperties = new File("conf/logging.properties");
            if (logProperties.exists())
                LogManager.getLogManager().readConfiguration(new FileInputStream(logProperties.getAbsolutePath()));
            else System.err.println("Could not find cond/logging.properties!");
        } catch (Exception err) {
            err.printStackTrace();
        }
    }

    public static Logger getLogger(String name) {
        Logger log = Logger.getLogger(name);
        if (log.getHandlers().length == 0) {
            ConsoleHandler handler = new ConsoleHandler();
            handler.setFormatter(new SingleLineFormatter());
            log.setUseParentHandlers(false);
            log.addHandler(handler);
        }
        return log;
    }

    // http://stackoverflow.com/questions/194765/how-do-i-get-java-logging-output-to-appear-on-a-single-line
    public static class SingleLineFormatter extends Formatter {

        Date dat = new Date();
        private final static String format = "{0,time}";
        private MessageFormat formatter;
        private Object args[] = new Object[1];

        // Line separator string.  This is the value of the line.separator
        // property at the moment that the SimpleFormatter was created.
        //private String lineSeparator = (String) java.security.AccessController.doPrivileged(
        //        new sun.security.action.GetPropertyAction("line.separator"));
        private String lineSeparator = "\n";

        /**
         * Format the given LogRecord.
         * @param record the log record to be formatted.
         * @return a formatted log record
         */
        public synchronized String format(LogRecord record) {

            StringBuilder sb = new StringBuilder();

            // Minimize memory allocations here.
            dat.setTime(record.getMillis());
            args[0] = dat;


            // Date and time
            StringBuffer text = new StringBuffer();
            if (formatter == null) {
                formatter = new MessageFormat(format);
            }
            formatter.format(args, text, null);
            sb.append(text);
            sb.append(" ");


            // Logger name
            sb.append(record.getLoggerName());

            // Method name
            if (record.getSourceMethodName() != null) {
                sb.append(" ");
                sb.append(record.getSourceMethodName());
            }
            sb.append(" - t:"); // lineSeparator

            // Thread name
            sb.append(Thread.currentThread().getId());
            sb.append(" ");

            String message = formatMessage(record);

            // Level
            sb.append(record.getLevel().getLocalizedName());
            sb.append(": ");

            // Indent - the more serious, the more indented.
            //sb.append( String.format("% ""s") );
            int iOffset = (1000 - record.getLevel().intValue()) / 100;
            for( int i = 0; i < iOffset;  i++ ){
                sb.append(" ");
            }


            sb.append(message);
            sb.append(lineSeparator);
            if (record.getThrown() != null) {
                try {
                    StringWriter sw = new StringWriter();
                    PrintWriter pw = new PrintWriter(sw);
                    record.getThrown().printStackTrace(pw);
                    pw.close();
                    sb.append(sw.toString());
                } catch (Exception ex) {
                }
            }
            return sb.toString();
        }
    }
}
