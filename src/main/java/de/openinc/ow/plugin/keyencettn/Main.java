package de.openinc.ow.plugin.keyencettn;

import de.openinc.api.OpenWareAPI;
import de.openinc.api.OpenWarePlugin;
import de.openinc.model.data.OpenWareDataItem;
import de.openinc.model.data.OpenWareNumber;
import de.openinc.model.data.OpenWareValue;
import de.openinc.model.data.OpenWareValueDimension;
import de.openinc.model.user.User;
import de.openinc.ow.OpenWareInstance;
import de.openinc.ow.helper.Config;
import de.openinc.ow.helper.HTTPResponseHelper;
import de.openinc.ow.middleware.services.DataService;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.javalin.apibuilder.ApiBuilder.post;

public class Main implements OpenWarePlugin, OpenWareAPI {

    boolean active = false;

    @Override
    public void registerRoutes() {
        // TODO Auto-generated method stub
        post("/keyencettn/push/{source}", ctx -> {

            User user = null;
            OpenWareInstance.getInstance().logTrace(String.format("[%s] %s)", this.getClass().getCanonicalName(), ctx.body()));
            String source = ctx.pathParam("source");
            if (Config.getBool("accessControl", true)) {
                user = ctx.sessionAttribute("user");
                if (user == null) {
                    HTTPResponseHelper.forbidden("Provide Authorization header or 'OD-SESSION' header to authorize");

                }

            }
            try {
                //ArrayList<OpenWareDataItem> results = new ArrayList<>();
                JSONObject payload = new JSONObject(ctx.body());
                JSONObject decodedFields = payload.getJSONObject("uplink_message").getJSONObject("decoded_payload");
                double rssi = payload.getJSONObject("uplink_message").getJSONArray("rx_metadata").getJSONObject(0).optDouble("rssi", 0);
                JSONObject loraIds = payload.getJSONObject("end_device_ids");
                loraIds.put("devHash", "_" + loraIds.getString("dev_eui"));

                String name = loraIds.getJSONObject("application_ids").getString("application_id") + "_" + loraIds.getString("dev_eui");
                String id = loraIds.getJSONObject("application_ids").getString("application_id") + "_" + loraIds.getString("dev_eui");

                if (decodedFields.has("idSuffix")) {
                    id = id + decodedFields.getString("idSuffix");
                    decodedFields.remove("idSuffix");
                }

                ArrayList<OpenWareValueDimension> vTypes = new ArrayList();
                long epochMilli = System.currentTimeMillis();
                OpenWareValue val = new OpenWareValue(epochMilli);
                OpenWareValue val10MinutesAgo = new OpenWareValue(epochMilli - 600000); // minus ten minutes
                OpenWareValue val20MinutesAgo = new OpenWareValue(epochMilli - 1200000); // minus twenty minutes
                decodedFields.keySet().stream().sorted().forEachOrdered(new Consumer<String>() {
                    @Override
                    public void accept(String key) {
                        if (key.endsWith("_unit")) return;
                        OpenWareValueDimension dim = OpenWareValueDimension.inferDimension(decodedFields.get(key), decodedFields.optString(key + "_unit"), key);

                        switch (key) {
                            case "intervalIdOneIntervalBefore":
                                dim.setName("intervalId");
                                val10MinutesAgo.add(dim);
                                break;
                            case "intervalIdTwoIntervalsBefore":
                                dim.setName("intervalId");
                                val20MinutesAgo.add(dim);
                                break;
                            case "flowOneIntervalBefore":
                                dim.setName("flow");
                                val10MinutesAgo.add(dim);
                                break;
                            case "flowTwoIntervalsBefore":
                                dim.setName("flow");
                                val20MinutesAgo.add(dim);
                                break;
                            case "intervalId":
                            case "flow":
                                val.add(dim);
                                vTypes.add(dim);
                                break;
                        }
                    }
                });
                if (!user.canAccessWrite(source, id)) {
                    HTTPResponseHelper.forbidden("You do not have permission to write this data source");

                }
                OpenWareDataItem item = new OpenWareDataItem(id, source, name, loraIds, vTypes);
                OpenWareDataItem item10MinutesAgo = new OpenWareDataItem(id, source, name, loraIds, vTypes);
                OpenWareDataItem item20MinutesAgo = new OpenWareDataItem(id, source, name, loraIds, vTypes);
                item.value().add(val);
                item10MinutesAgo.value().add(val10MinutesAgo);
                item20MinutesAgo.value().add(val20MinutesAgo);

                OpenWareInstance.getInstance().logTrace("item = " + item);
                OpenWareInstance.getInstance().logTrace("item10MinutesAgo = " + item10MinutesAgo);
                OpenWareInstance.getInstance().logTrace("item20MinutesAgo = " + item20MinutesAgo);

                OpenWareDataItem existingItemsLastHalfHour = DataService.getHistoricalSensorData(name, source, epochMilli - 1800000, epochMilli);
                OpenWareInstance.getInstance().logTrace("existingItemsLastHalfHour = " + existingItemsLastHalfHour);

                List<OpenWareValue> valuesLastHalfHour = new ArrayList<OpenWareValue>();
                if (existingItemsLastHalfHour != null) {
                    valuesLastHalfHour = existingItemsLastHalfHour.value();
                }

                boolean deliverOneIntervalBefore = true;
                boolean deliverTwoIntervalsBefore = true;

                for (OpenWareValue value : valuesLastHalfHour) {
                    OpenWareInstance.getInstance().logTrace("OpenWareValue from last half hour = " + value);
                    OpenWareValueDimension dim = value.get(0);
                    OpenWareInstance.getInstance().logTrace("OpenWareValueDimension = " + dim);
                    double intervalId = ((OpenWareNumber) dim).value();
                    OpenWareInstance.getInstance().logTrace("intervalId = " + intervalId);
                    OpenWareInstance.getInstance().logTrace("((OpenWareNumber)val10MinutesAgo.get(0)).value() = " + ((OpenWareNumber) val10MinutesAgo.get(0)).value());
                    OpenWareInstance.getInstance().logTrace("((OpenWareNumber)val20MinutesAgo.get(0)).value() = " + ((OpenWareNumber) val20MinutesAgo.get(0)).value());
                    if (intervalId == ((OpenWareNumber) val10MinutesAgo.get(0)).value()) {
                        OpenWareInstance.getInstance().logTrace("intervalID " + intervalId + " from 10 minutes ago has already been delivered, value = " + ((OpenWareNumber) val10MinutesAgo.get(0)).value());
                        deliverOneIntervalBefore = false;
                    }
                    if (intervalId == ((OpenWareNumber) val20MinutesAgo.get(0)).value()) {
                        OpenWareInstance.getInstance().logTrace("intervalID " + intervalId + " from 20 minutes ago has already been delivered, value = " + ((OpenWareNumber) val20MinutesAgo.get(0)).value());
                        deliverTwoIntervalsBefore = false;
                    }
                }

                if (deliverOneIntervalBefore) {
                    OpenWareInstance.getInstance().logTrace("No value was delivered 10 minutes ago (" + (epochMilli - 600000) + "), deliver now");
                    DataService.onNewData(item10MinutesAgo);
                }

                if (deliverTwoIntervalsBefore) {
                    OpenWareInstance.getInstance().logTrace("No value was delivered 20 minutes ago (" + (epochMilli - 1200000) + "), deliver now");
                    DataService.onNewData(item20MinutesAgo);
                }

                DataService.onNewData(item);

                OpenWareInstance.getInstance().logTrace("Before HTTPResponseHelper.ok(ctx, item);");
                HTTPResponseHelper.ok(ctx, item);

            } catch (JSONException e) {
                OpenWareInstance.getInstance().logError("Malformed data posted to Keyence TTN Integration API\n" + ctx.body(), e);
                HTTPResponseHelper.badRequest("Malformed data posted to Keyence TTN-LORA API\n" + ctx.body());

            }

        });

    }

    @Override
    public boolean init(OpenWareInstance instance, JSONObject options) throws Exception {
        if (options != null) {
            active = options.optBoolean("enabled");
        }
        if (active) {
            instance.registerService(this);
        }

        return true;
    }

    @Override
    public boolean disable() throws Exception {
        // TODO Auto-generated method stub
        return false;
    }

}
