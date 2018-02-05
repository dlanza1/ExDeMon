package ch.cern.log4j;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.EnumSet;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.regex.Pattern;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.log4j.Appender;
import org.apache.log4j.Category;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.AppenderAttachable;
import org.apache.log4j.spi.LocationInfo;
import org.apache.log4j.spi.LoggingEvent;
import org.apache.log4j.spi.ThrowableInformation;

public class JsonLayout extends Layout {

    private static final Pattern SEP_PATTERN = Pattern.compile("(?:\\p{Space}*?[,;]\\p{Space}*)+");
    private static final Pattern PAIR_SEP_PATTERN = Pattern.compile("(?:\\p{Space}*?[:=]\\p{Space}*)+");

    private static final char[] HEX_CHARS =
        {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    private enum LocationField {
        CLASS("class"),
        FILE("file"),
        LINE("line"),
        METHOD("method");

        private final String val;

        LocationField(String val) {
            this.val = val;
        }
    }

    private enum ExceptionField {
        CLASS("class"),
        MESSAGE("message"),
        STACKTRACE("stacktrace");

        private final String val;

        ExceptionField(String val) {
            this.val = val;
        }
    }

    private enum Field {
        EXCEPTION("exception"),
        LEVEL("level"),
        LOCATION("location"),
        LOGGER("logger"),
        MESSAGE("message"),
        MDC("mdc"),
        NDC("ndc"),
        HOST("host"),
        PATH("path"),
        TAGS("tags"),
        TIMESTAMP("@timestamp"),
        THREAD("thread"),
        VERSION("@version");

        private final String val;

        Field(String exception) {
            val = exception;
        }

        public static Field fromValue(String val) {
            for (Field field : values()) {
                if (field.val.equals(val)) {
                    return field;
                }
            }
            throw new IllegalArgumentException(
                String.format("Unsupported value [%s]. Expecting one of %s.", val, Arrays.toString(values())));
        }
    }

    private static final String VERSION = "1";

    private String tagsVal;
    private String fieldsVal;
    private String includedFields;
    private String excludedFields;

    private final Map<String, String> fields;
    private final Set<Field> renderedFields;
    private final DateFormat dateFormat;
    private final Date date;
    private final StringBuilder buf;

    private String[] tags;
    private String path;
    private boolean pathResolved;
    private String hostName;
    private boolean ignoresThrowable;

    public JsonLayout() {
        fields = new HashMap<String, String>();

        renderedFields = EnumSet.allOf(Field.class);
        renderedFields.remove(Field.LOCATION);

        dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));

        date = new Date();
        buf = new StringBuilder(32*1024);
    }

    @Override
    public String format(LoggingEvent event) {
        buf.setLength(0);

        buf.append('{');

        boolean hasPrevField = false;
        if (renderedFields.contains(Field.EXCEPTION)) {
            hasPrevField = appendException(buf, event);
        }

        if (hasPrevField) {
            buf.append(',');
        }
        hasPrevField = appendFields(buf, event);

        if (renderedFields.contains(Field.LEVEL)) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, Field.LEVEL.val, event.getLevel().toString());
            hasPrevField = true;
        }

        if (renderedFields.contains(Field.LOCATION)) {
            if (hasPrevField) {
                buf.append(',');
            }
            hasPrevField = appendLocation(buf, event);
        }

        if (renderedFields.contains(Field.LOGGER)) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, Field.LOGGER.val, event.getLoggerName());
            hasPrevField = true;
        }

        if (renderedFields.contains(Field.MESSAGE)) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, Field.MESSAGE.val, event.getRenderedMessage());
            hasPrevField = true;
        }

        if (renderedFields.contains(Field.MDC)) {
            if (hasPrevField) {
                buf.append(',');
            }
            hasPrevField = appendMDC(buf, event);
        }

        if (renderedFields.contains(Field.NDC)) {
            String ndc = event.getNDC();
            if (ndc != null && !ndc.isEmpty()) {
                if (hasPrevField) {
                    buf.append(',');
                }
                appendField(buf, Field.NDC.val, event.getNDC());
                hasPrevField = true;
            }
        }

        if (renderedFields.contains(Field.HOST)) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, Field.HOST.val, hostName);
            hasPrevField = true;
        }

        if (renderedFields.contains(Field.PATH)) {
            if (hasPrevField) {
                buf.append(',');
            }
            hasPrevField = appendSourcePath(buf, event);
        }

        if (renderedFields.contains(Field.TAGS)) {
            if (hasPrevField) {
                buf.append(',');
            }
            hasPrevField = appendTags(buf, event);
        }

        if (renderedFields.contains(Field.TIMESTAMP)) {
            if (hasPrevField) {
                buf.append(',');
            }
            date.setTime(event.getTimeStamp());
            appendField(buf, Field.TIMESTAMP.val, dateFormat.format(date));
            hasPrevField = true;
        }

        if (renderedFields.contains(Field.THREAD)) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, Field.THREAD.val, event.getThreadName());
            hasPrevField = true;
        }

        if (renderedFields.contains(Field.VERSION)) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, Field.VERSION.val, VERSION);
        }

        buf.append("}");

        return buf.toString();
    }

    private boolean appendFields(StringBuilder buf, LoggingEvent event) {
        if (fields.isEmpty()) {
            return false;
        }

        for (Iterator<Map.Entry<String, String>> iter = fields.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<String, String> entry = iter.next();
            appendField(buf, entry.getKey(), entry.getValue());
            if (iter.hasNext()) {
                buf.append(',');
            }
        }

        return true;
    }

    private boolean appendSourcePath(StringBuilder buf, LoggingEvent event) {
        if (!pathResolved) {
            Appender appender = findLayoutAppender(event.getLogger());
            if (appender instanceof FileAppender) {
                FileAppender fileAppender = (FileAppender) appender;
                path = getAppenderPath(fileAppender);
            }
            pathResolved = true;
        }
        if (path != null) {
            appendField(buf, Field.PATH.val, path);
            return true;
        }
        return false;
    }

    private Appender findLayoutAppender(Category logger) {
        for(Category parent = logger; parent != null; parent = parent.getParent()) {
            @SuppressWarnings("unchecked")
            Appender appender = findLayoutAppender(parent.getAllAppenders());
            if(appender != null) {
                return appender;
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private Appender findLayoutAppender(Enumeration<? extends Appender> appenders) {
        if(appenders == null) {
            return null;
        }

        while (appenders.hasMoreElements()) {
            Appender appender = appenders.nextElement();
            // get the first appender with this layout instance and ignore others;
            // actually a single instance of this class is not intended to be used with multiple threads.
            if (appender.getLayout() == this) {
                return appender;
            }
            if (appender instanceof AppenderAttachable) {
                AppenderAttachable appenderContainer = (AppenderAttachable) appender;
                return findLayoutAppender(appenderContainer.getAllAppenders());
            }
        }
        return null;
    }

    private String getAppenderPath(FileAppender fileAppender) {
        String path = null;
        try {
            String fileName = fileAppender.getFile();
            if (fileName != null && !fileName.isEmpty()) {
                path = new File(fileName).getCanonicalPath();
            }
        } catch (IOException e) {
            LogLog.error("Unable to retrieve appender's file name", e);
        }
        return path;
    }

    private boolean appendTags(StringBuilder builder, LoggingEvent event) {
        if (tags == null || tags.length == 0) {
            return false;
        }

        appendQuotedName(builder, Field.TAGS.val);
        builder.append(":[");
        for (int i = 0, len = tags.length; i < len; i++) {
            appendQuotedValue(builder, tags[i]);
            if (i != len - 1) {
                builder.append(',');
            }
        }
        builder.append(']');

        return true;
    }

    private boolean appendMDC(StringBuilder buf, LoggingEvent event) {
        Map<?, ?> entries = event.getProperties();
        if (entries.isEmpty()) {
            return false;
        }

        appendQuotedName(buf, Field.MDC.val);
        buf.append(":{");

        for (Iterator<? extends Map.Entry<?, ?>> iter = entries.entrySet().iterator(); iter.hasNext(); ) {
            Map.Entry<?, ?> entry = iter.next();
            appendField(buf, entry.getKey(), entry.getValue());
            if (iter.hasNext()) {
                buf.append(',');
            }
        }
        buf.append('}');

        return true;
    }

    private boolean appendLocation(StringBuilder buf, LoggingEvent event) {
        LocationInfo locationInfo = event.getLocationInformation();
        if (locationInfo == null) {
            return false;
        }

        boolean hasPrevField = false;

        appendQuotedName(buf, Field.LOCATION.val);
        buf.append(":{");

        String className = locationInfo.getClassName();
        if (className != null) {
            appendField(buf, LocationField.CLASS.val, className);
            hasPrevField = true;
        }

        String fileName = locationInfo.getFileName();
        if (fileName != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, LocationField.FILE.val, fileName);
            hasPrevField = true;
        }

        String methodName = locationInfo.getMethodName();
        if (methodName != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, LocationField.METHOD.val, methodName);
            hasPrevField = true;
        }

        String lineNum = locationInfo.getLineNumber();
        if (lineNum != null) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendField(buf, LocationField.LINE.val, lineNum);
        }

        buf.append('}');

        return true;
    }

    private boolean appendException(StringBuilder buf, LoggingEvent event) {
        ThrowableInformation throwableInfo = event.getThrowableInformation();
        if (throwableInfo == null) {
            return false;
        }

        appendQuotedName(buf, Field.EXCEPTION.val);
        buf.append(":{");

        boolean hasPrevField = false;

        Throwable throwable = throwableInfo.getThrowable();
        if (throwable != null) {
            String message = throwable.getMessage();
            if (message != null) {
                appendField(buf, ExceptionField.MESSAGE.val, message);
                hasPrevField = true;
            }

            String className = throwable.getClass().getCanonicalName();
            if (className != null) {
                if (hasPrevField) {
                    buf.append(',');
                }
                appendField(buf, ExceptionField.CLASS.val, className);
                hasPrevField = true;
            }
        }

        String[] stackTrace = throwableInfo.getThrowableStrRep();
        if (stackTrace != null && stackTrace.length != 0) {
            if (hasPrevField) {
                buf.append(',');
            }
            appendQuotedName(buf, ExceptionField.STACKTRACE.val);
            buf.append(":\"");
            for (int i = 0, len = stackTrace.length; i < len; i++) {
                appendValue(buf, stackTrace[i]);
                if (i != len - 1) {
                    appendChar(buf, '\n');
                }
            }
            buf.append('\"');
        }

        buf.append('}');

        return true;
    }

    @Override
    public boolean ignoresThrowable() {
        return ignoresThrowable;
    }

    public void activateOptions() {
        if (includedFields != null) {
            String[] included = SEP_PATTERN.split(includedFields);
            for (String val : included) {
                renderedFields.add(Field.fromValue(val));
            }
        }
        if (excludedFields != null) {
            String[] excluded = SEP_PATTERN.split(excludedFields);
            for (String val : excluded) {
                renderedFields.remove(Field.fromValue(val));
            }
        }
        if (tagsVal != null) {
            tags = SEP_PATTERN.split(tagsVal);
        }
        if (fieldsVal != null) {
            String[] fields = SEP_PATTERN.split(fieldsVal);
            for (String fieldVal : fields) {
                String[] field = PAIR_SEP_PATTERN.split(fieldVal);
                this.fields.put(field[0], field[1]);
            }
        }
        if (hostName == null) {
            try {
                hostName = InetAddress.getLocalHost().getHostName();
            } catch (UnknownHostException e) {
                hostName = "localhost";
                LogLog.error("Unable to determine name of the localhost", e);
            }
        }
        ignoresThrowable = !renderedFields.contains(Field.EXCEPTION);
    }

    @Override
    public String getContentType() {
        return "application/json";
    }

    private void appendQuotedName(StringBuilder out, Object name) {
        out.append('\"');
        appendValue(out, String.valueOf(name));
        out.append('\"');
    }

    private void appendQuotedValue(StringBuilder out, Object val) {
        out.append('\"');
        appendValue(out, String.valueOf(val));
        out.append('\"');
    }

    private void appendValue(StringBuilder out, String val) {
        for (int i = 0, len = val.length(); i < len; i++) {
            appendChar(out, val.charAt(i));
        }
    }

    private void appendField(StringBuilder out, Object name, Object val) {
        appendQuotedName(out, name);
        out.append(':');
        appendQuotedValue(out, val);
    }

    private void appendChar(StringBuilder out, char ch) {
        switch (ch) {
            case '"':
                out.append("\\\"");
                break;
            case '\\':
                out.append("\\\\");
                break;
            case '/':
                out.append("\\/");
                break;
            case '\b':
                out.append("\\b");
                break;
            case '\f':
                out.append("\\f");
                break;
            case '\n':
                out.append("\\n");
                break;
            case '\r':
                out.append("\\r");
                break;
            case '\t':
                out.append("\\t");
                break;
            default:
                if ((ch <= '\u001F') || ('\u007F' <= ch && ch <= '\u009F') || ('\u2000' <= ch && ch <= '\u20FF')) {
                    out.append("\\u")
                        .append(HEX_CHARS[ch >> 12 & 0x000F])
                        .append(HEX_CHARS[ch >> 8 & 0x000F])
                        .append(HEX_CHARS[ch >> 4 & 0x000F])
                        .append(HEX_CHARS[ch & 0x000F]);
                } else {
                    out.append(ch);
                }
                break;
        }
    }

    public void setTags(String tags) {
        this.tagsVal = tags;
    }

    public void setFields(String fields) {
        this.fieldsVal = fields;
    }

    public void setIncludedFields(String includedFields) {
        this.includedFields = includedFields;
    }

    public void setExcludedFields(String excludedFields) {
        this.excludedFields = excludedFields;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }
}