/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.opensearch.model;

import java.io.Serializable;
import java.util.List;

/**
 * Reference DeletePITResponse Object class
 */
public class DeletePitResponse implements Serializable {

    Integer code;
    List<PitInformation> pits;

    public static class PitInformation {
        private Boolean successful;

        private String pit_id;

        public Boolean getSuccessful() {
            return successful;
        }

        public void setSuccessful(Boolean successful) {
            this.successful = successful;
        }

        public String getPitId() {
            return pit_id;
        }

        public void setPitId(String pit_id) {
            this.pit_id = pit_id;
        }


        @Override
        public String toString() {
            return "PitInformation{" +
                    "successful=" + successful +
                    ", pit_id='" + pit_id + '\'' +
                    '}';
        }
    }

    public List<PitInformation> getPits() {
        return pits;
    }

    public void setPits(List<PitInformation> pits) {
        this.pits = pits;
    }


    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    @Override
    public String toString() {
        return "DeletePITResponse{" +
                "code=" + code +
                ", pits=" + pits +
                '}';
    }
}
