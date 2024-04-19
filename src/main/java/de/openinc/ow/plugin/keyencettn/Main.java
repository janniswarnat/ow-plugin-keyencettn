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

//				String timestamp = payload.getString("received_at");
//				OpenWareInstance.getInstance()
//						.logTrace("timestamp from payload.data.received_at = "+timestamp);
//				ZonedDateTime zonedDateTime = ZonedDateTime.parse(timestamp, DateTimeFormatter.ISO_DATE_TIME);
//
//				// Calculate rounding to the nearest ten minutes
//				int minute = zonedDateTime.getMinute();
//				int roundedMinutes = ((minute + 5) / 10) * 10; // Adding 5 before division for rounding
//				zonedDateTime = zonedDateTime
//						.with(ChronoField.MINUTE_OF_HOUR, roundedMinutes % 60) // Ensure minutes wrap correctly
//						.with(ChronoField.SECOND_OF_MINUTE, 0)
//						.with(ChronoField.NANO_OF_SECOND, 0);
//
//				// Adjust the hour if necessary (when rounding occurs at xx:55 to xx:00)
//				if (roundedMinutes == 60) {
//					zonedDateTime = zonedDateTime.plusHours(1);
//				}
//
//				// Convert to epoch milliseconds directly
//				long epochMilli = zonedDateTime.toInstant().toEpochMilli();
//				OpenWareInstance.getInstance()
//						.logTrace("epochMilli = "+epochMilli);

                if (decodedFields.has("idSuffix")) {
                    id = id + decodedFields.getString("idSuffix");
                    decodedFields.remove("idSuffix");
                }

//				if (decodedFields.has("latitude") || decodedFields.has("lat")) {
//					if (decodedFields.has("longitude") || decodedFields.has("lon")) {
//						String template = "{\r\n" + "      \"type\": \"Feature\",\r\n" + "      \"geometry\": {\r\n"
//								+ "        \"type\": \"Point\",\r\n" + "        \"coordinates\": [%s, %s, %s]\r\n"
//								+ "      },\r\n" + "      \"properties\": {\r\n" + "        \"name\": %s,\r\n"
//								+ "        \"rssi\": %s\r\n" + "        \r\n" + "      }\r\n" + "    }   ";
//						double lon = decodedFields.has("lon") ? decodedFields.getDouble("lon")
//								: decodedFields.getDouble("longitude");
//						double lat = decodedFields.has("lat") ? decodedFields.getDouble("lat")
//								: decodedFields.getDouble("latitude");
//						double alt = decodedFields.has("alt") ? decodedFields.getDouble("alt") : 0.0;
//						if (alt == 0.0) {
//							alt = decodedFields.has("altitude") ? decodedFields.getDouble("altitude") : 0.0;
//						}
//						String geo = String.format(template, lon, lat, alt, name, rssi);
//						JSONObject geoJson = new JSONObject(geo);
//						decodedFields.remove("latitude");
//						decodedFields.remove("lat");
//						decodedFields.remove("lon");
//						decodedFields.remove("longitude");
//						decodedFields.remove("alt");
//						decodedFields.remove("altitude");
//						decodedFields.put("parsedGeo", geoJson);
//					}
//				}
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
                            case "litersOneIntervalBefore":
                                dim.setName("liters");
                                val10MinutesAgo.add(dim);
                                break;
                            case "litersTwoIntervalsBefore":
                                dim.setName("liters");
                                val20MinutesAgo.add(dim);
                                break;
                            case "intervalId":
                            case "liters":
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

                //item.streamPrint(System.out);
                //item10MinutesAgo.streamPrint(System.out);
                //item20MinutesAgo.streamPrint(System.out);

                OpenWareDataItem existingItemsLastHalfHour = DataService.getHistoricalSensorData(name, source, epochMilli - 1800000, epochMilli);
                //OpenWareDataItem existingItem10MinutesAgo = DataService.getHistoricalSensorData(name, source, epochMilli-600000, epochMilli-600000);
                //OpenWareDataItem existingItem20MinutesAgo = DataService.getHistoricalSensorData(name, source, epochMilli-1200000, epochMilli-1200000);

                OpenWareInstance.getInstance().logTrace("existingItemsLastHalfHour = " + existingItemsLastHalfHour);
//				OpenWareInstance.getInstance()
//						.logTrace("existingIItem10MinutesAgo = "+existingItem10MinutesAgo);
//				OpenWareInstance.getInstance()
//						.logTrace("existingIItem20MinutesAgo = "+existingItem20MinutesAgo);

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

//				List<OpenWareValue> values10MinutesAgo = new ArrayList<OpenWareValue>();
//				if(existingItem10MinutesAgo != null)
//				{
//					values10MinutesAgo = existingItem10MinutesAgo.value();
//				}
//
//				if(!values10MinutesAgo.isEmpty())
//				{
//					OpenWareValueDimension dim10MinutesAgo = values10MinutesAgo.get(0).get(0);
//					double liters10MinutesAgo = ((OpenWareNumber) dim10MinutesAgo).value();
//
////					if(liters10MinutesAgo == 0.0)
////					{
////						OpenWareInstance.getInstance()
////								.logTrace("Zero was delivered 10 minutes ago ("+(epochMilli-600000)+"), replace");
////						DataService.onNewData(item10MinutesAgo);
////					}
////					else
////					{
//						OpenWareInstance.getInstance()
//								.logTrace("Some value was delivered 10 minutes ago ("+(epochMilli-600000)+"), do not replace");
////					}
//				}
//				else
//				{
//					OpenWareInstance.getInstance()
//							.logTrace("No value was delivered 10 minutes ago ("+(epochMilli-600000)+")");
//					DataService.onNewData(item10MinutesAgo);
//				}

//				List<OpenWareValue> values20MinutesAgo = new ArrayList<OpenWareValue>();
//				if(existingItem20MinutesAgo != null)
//				{
//					values20MinutesAgo = existingItem20MinutesAgo.value();
//				}
//
//				if(!values20MinutesAgo.isEmpty())
//				{
//					OpenWareValueDimension dim20MinutesAgo = values20MinutesAgo.get(0).get(0);
//					double liters20MinutesAgo = ((OpenWareNumber) dim20MinutesAgo).value();
//
////					if(liters20MinutesAgo == 0.0)
////					{
////						OpenWareInstance.getInstance()
////								.logTrace("Zero was delivered 20 minutes ago ("+(epochMilli-1200000)+"), replace");
////						DataService.onNewData(item20MinutesAgo);
////					}
////					else
////					{
//						OpenWareInstance.getInstance()
//								.logTrace("Some value was delivered 20 minutes ago ("+(epochMilli-1200000)+"), do not replace");
////					}
//				}
//				else
//				{
//					OpenWareInstance.getInstance()
//							.logTrace("No value was delivered 20 minutes ago ("+(epochMilli-1200000)+")");
//					DataService.onNewData(item20MinutesAgo);
//				}


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
