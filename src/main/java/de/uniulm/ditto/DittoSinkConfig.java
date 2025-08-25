package de.uniulm.ditto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Strings;
import org.apache.pulsar.io.core.annotations.FieldDoc;

import java.io.IOException;
import java.io.Serializable;
import java.util.Map;

public class DittoSinkConfig implements Serializable {
    @FieldDoc(
            required = true,
            defaultValue = "ditto",
            help = "The username that will be used to authenticate with the Ditto server." +
                    "The default value is aligned to the Ditto documentation: 'ditto'.")
    String dittoUsername;

    @FieldDoc(
            required = true,
            defaultValue = "ditto",
            help = "The password that will be used to authenticate with the Ditto server." +
                    "The default value is aligned to the Ditto documentation: 'ditto'.")
    String dittoPassword;

    @FieldDoc(
            required = true,
            defaultValue = "",
            help = "")
    String websocketEndpoint;

    public static DittoSinkConfig load(Map<String, Object> map) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(mapper.writeValueAsString(map), DittoSinkConfig.class);
    }

    public String getDittoUsername() {
        return dittoUsername;
    }

    public void setDittoUsername(String dittoUsername) {
        this.dittoUsername = dittoUsername;
    }

    public String getDittoPassword() {
        return dittoPassword;
    }

    public void setDittoPassword(String dittoPassword) {
        this.dittoPassword = dittoPassword;
    }

    public String getWebsocketEndpoint() {
        return websocketEndpoint;
    }

    public void setWebsocketEndpoint(String websocketEndpoint) {
        this.websocketEndpoint = websocketEndpoint;
    }
}
