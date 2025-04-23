package com.mergemesh.mongo_server.shared;

import java.util.List;

public interface Server {

    /**
     * Query the obtained marks/grade of a student.
     *
     * @param studentId Unique identifier for the student
     * @return Obtained marks or grade as a string (could be JSON or raw value)
     */
    String query(String studentId);

    /**
     * Update the obtained marks/grade of a student.
     *
     * @param studentId Unique identifier for the student
     * @param value     New marks/grade
     * @return Confirmation message or status
     */
    String update(String studentId, String value);

    /**
     * Merge current system's state with another system by replaying its oplog.
     *
     * @param sourceSystemUrl Base URL of the other system's server (e.g., http://mongo-server:8080)
     * @return Merge result or summary
     */
    String merge(String sourceSystemUrl);

    /**
     * Get the local operation log for this system.
     * This can be used by other servers to perform merges.
     *
     * @return List of OplogEntry objects representing the operation history
     */
    List<OplogEntry> getOpLog();
}
