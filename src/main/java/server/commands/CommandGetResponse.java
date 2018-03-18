/**
 * @author Dilip Simha
 */
package server.commands;

import server.cache.CacheValue;

/** Response for "get" command. This will be returned to client */
public class CommandGetResponse {
    final CacheValue value;

    public CommandGetResponse(CacheValue value) {
        this.value = value;
    }
}
