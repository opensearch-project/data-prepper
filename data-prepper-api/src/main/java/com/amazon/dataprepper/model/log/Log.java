package com.amazon.dataprepper.model.log;

import com.amazon.dataprepper.model.event.Event;

/**
 * A log event in Data Prepper represents a single log line. A log event does not require any specific keys allowing this event type to
 * support any log structure.
 * @since 1.2
 */
public interface Log extends Event {

}
