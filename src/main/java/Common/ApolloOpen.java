package Common;

import com.ctrip.framework.apollo.openapi.client.ApolloOpenApiClient;

public class ApolloOpen {
    private ApolloOpenApiClient client;
    public ApolloOpenApiClient getClient() {
        return client;
    }
    public ApolloOpen(String url,String token) {
        client = ApolloOpenApiClient.newBuilder()
                .withPortalUrl(url)
                .withToken(token)
                .build();
    }
}
