package com.xiaomi.infra.pegasus.spark.common.utils.gateway;

import java.io.Serializable;
import java.util.Map;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TableDupInfo implements Serializable {
    public class DupInfo {
        public String create_ts;
        public int dupid;
        public String fail_mode;
        public String remote;
        public String status;

        @Override
        public String toString() {
            return "DupInfo{" +
                    "create_ts='" + create_ts + '\'' +
                    ", dupid=" + dupid +
                    ", fail_mode='" + fail_mode + '\'' +
                    ", remote='" + remote + '\'' +
                    ", status='" + status + '\'' +
                    '}';
        }
    }

    public Map<Integer, TableDupInfo.DupInfo> duplications;
    public Integer appid;

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TableDupInfo{");
        sb.append("appid=").append(appid).append(", ");
        sb.append("duplications=[");
        if (duplications!= null) {
            for (Map.Entry<Integer, TableDupInfo.DupInfo> entry : duplications.entrySet()) {
                sb.append(entry.getKey().toString()).append(" : ");
                sb.append(entry.getValue().toString()).append(", ");
            }
        }
        if (sb.charAt(sb.length() - 2) == ',') {
            sb.delete(sb.length() - 2, sb.length());
        }
        sb.append("]}");
        return sb.toString();
    }


    public static TableDupInfo fromJson(String json) {
        Gson gson = new Gson();
        JsonObject jsonObject = gson.fromJson(json, JsonObject.class);
        TableDupInfo tableDupInfo = new TableDupInfo();
        tableDupInfo.appid = jsonObject.get("appid").getAsInt();

        tableDupInfo.duplications = new HashMap<>();
        JsonObject duplicationsObject = jsonObject.getAsJsonObject();
        for (Map.Entry<String, JsonElement> entry : duplicationsObject.entrySet()) {
            if (!entry.getKey().equals("appid")) {
                JsonObject dupInfoObject = entry.getValue().getAsJsonObject();
                DupInfo dupInfo = tableDupInfo.new DupInfo();
                dupInfo.create_ts = dupInfoObject.get("create_ts").getAsString();
                dupInfo.dupid = dupInfoObject.get("dupid").getAsInt();
                dupInfo.fail_mode = dupInfoObject.get("fail_mode").getAsString();
                dupInfo.remote = dupInfoObject.get("remote").getAsString();
                dupInfo.status = dupInfoObject.get("status").getAsString();

                tableDupInfo.duplications.put(Integer.parseInt(entry.getKey()), dupInfo);
            }
        }
        return tableDupInfo;
    }
}
