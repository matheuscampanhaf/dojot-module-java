package com.cpqd.app.auth;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.JsonNode;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;
import com.cpqd.app.config.Config;
import org.apache.commons.codec.binary.Base64;
import org.json.JSONArray;
import org.json.JSONException;

import java.util.ArrayList;

public class Auth {

    private static Auth mInstance;

    private Auth() {}

    public static synchronized Auth getInstance() {
        if (mInstance == null) {
            mInstance = new Auth();
        }
        return mInstance;
    }

    /**
     * Generate token to be used in internal communication.
     *
     * @param tenant is the dojot tenant for which the token should be valid for
     * @return JWT token to be used in the requests
     */
    public String getToken(String tenant) {
        StringBuffer payload = new StringBuffer("{\"service\":\"");
        payload.append(tenant);
        payload.append("\",\"username\":\"iotagent\"}");

        System.out.println(payload);

        StringBuffer response = new StringBuffer(Base64.encodeBase64String("jwt schema".getBytes()));
        response.append(".");
        response.append(Base64.encodeBase64String(payload.toString().getBytes()));
        response.append(".");
        response.append(Base64.encodeBase64String("dummy signature".getBytes()));

        return response.toString();
    }

    /**
     * Gets all tenants that are registered on Auth service.
     *
     * @return List of tenants.
     */
    public ArrayList<String> getTenants(){
        StringBuffer url = new StringBuffer(Config.getInstance().getAuthAddress());
        url.append("/admin/tenants");
        ArrayList<String> resTenants = new ArrayList<>();
        try {
            HttpResponse<JsonNode> request = Unirest.get(url.toString()).header("authorization","Bearer " + Auth.getInstance().getToken(Config.getInstance().getInternalTenant())).asJson();
            JSONArray jsonArrayResponse = request.getBody().getObject().getJSONArray("tenants");
            for(int i = 0;i < jsonArrayResponse.length();i++) {
                resTenants.add(jsonArrayResponse.get(i).toString());
            }
        } catch(UnirestException exception) {
                return resTenants;
        } catch (JSONException exception){
                return resTenants;
        }
        System.out.println("resTenants:::: " + resTenants);
        return resTenants;
    }
}
