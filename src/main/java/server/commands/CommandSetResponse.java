/**
 * @author Dilip Simha
 */
package server.commands;

/** Response for "set" command. This will be returned to client */
public enum CommandSetResponse {
    STORED,
    NOT_STORED,
    /* rest of the return values do not apply for SET command */
    EXISTS,
    NOT_FOUND;
}
