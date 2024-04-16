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

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import static io.javalin.apibuilder.ApiBuilder.post;
import static java.time.temporal.ChronoUnit.MINUTES;

public class Main implements OpenWarePlugin, OpenWareAPI {

	boolean active = false;

	@Override
	public void registerRoutes() {
		// TODO Auto-generated method stub
		post("/keyencettn/push/{source}", ctx -> {

			User user = null;
			OpenWareInstance.getInstance()
					.logTrace(String.format("[%s] %s)", this.getClass().getCanonicalName(), ctx.body()));
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
				double rssi = payload.getJSONObject("uplink_message").getJSONArray("rx_metadata").getJSONObject(0)
						.optDouble("rssi", 0);
				JSONObject loraIds = payload.getJSONObject("end_device_ids");
				loraIds.put("devHash", "_" + loraIds.getString("dev_eui"));

				String name = loraIds.getJSONObject("application_ids").getString("application_id") + "_"
						+ loraIds.getString("dev_eui");
				String id = loraIds.getJSONObject("application_ids").getString("application_id") + "_"
						+ loraIds.getString("dev_eui");

				String timestamp = payload.getString("received_at");
				OpenWareInstance.getInstance()
						.logTrace("timestamp from payload.data.received_at = "+timestamp);
				Instant instant = Instant.parse(timestamp);
				instant = instant.truncatedTo(MINUTES);
				long epochMilli = instant.toEpochMilli();
				OpenWareInstance.getInstance()
						.logTrace("epochMilli = "+epochMilli);

				if (decodedFields.has("idSuffix")) {
					id = id + decodedFields.getString("idSuffix");
					decodedFields.remove("idSuffix");
				}

				if (decodedFields.has("latitude") || decodedFields.has("lat")) {
					if (decodedFields.has("longitude") || decodedFields.has("lon")) {
						String template = "{\r\n" + "      \"type\": \"Feature\",\r\n" + "      \"geometry\": {\r\n"
								+ "        \"type\": \"Point\",\r\n" + "        \"coordinates\": [%s, %s, %s]\r\n"
								+ "      },\r\n" + "      \"properties\": {\r\n" + "        \"name\": %s,\r\n"
								+ "        \"rssi\": %s\r\n" + "        \r\n" + "      }\r\n" + "    }   ";
						double lon = decodedFields.has("lon") ? decodedFields.getDouble("lon")
								: decodedFields.getDouble("longitude");
						double lat = decodedFields.has("lat") ? decodedFields.getDouble("lat")
								: decodedFields.getDouble("latitude");
						double alt = decodedFields.has("alt") ? decodedFields.getDouble("alt") : 0.0;
						if (alt == 0.0) {
							alt = decodedFields.has("altitude") ? decodedFields.getDouble("altitude") : 0.0;
						}
						String geo = String.format(template, lon, lat, alt, name, rssi);
						JSONObject geoJson = new JSONObject(geo);
						decodedFields.remove("latitude");
						decodedFields.remove("lat");
						decodedFields.remove("lon");
						decodedFields.remove("longitude");
						decodedFields.remove("alt");
						decodedFields.remove("altitude");
						decodedFields.put("parsedGeo", geoJson);
					}
				}
				ArrayList<OpenWareValueDimension> vTypes = new ArrayList();
				OpenWareValue val = new OpenWareValue(epochMilli);
				OpenWareValue val10MinutesAgo = new OpenWareValue(epochMilli-600000); // minus ten minutes
				OpenWareValue val20MinutesAgo = new OpenWareValue(epochMilli-1200000); // minus twenty minutes
				decodedFields.keySet().stream().sorted().forEachOrdered(new Consumer<String>() {
					@Override
					public void accept(String key) {
						if (key.endsWith("_unit"))
							return;
						OpenWareValueDimension dim = OpenWareValueDimension.inferDimension(decodedFields.get(key),
								decodedFields.optString(key + "_unit"), key);

						switch (key) {
//							case "flow":
//								val.add(dim);
//								vTypes.add(dim);
//								break;
							case "litersOneIntervalBefore":
								dim.setName("flow");
								val10MinutesAgo.add(dim);
								break;
							case "litersTwoIntervalsBefore":
								dim.setName("flow");
								val20MinutesAgo.add(dim);
								break;
							default:
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

				OpenWareInstance.getInstance()
						.logTrace("item = "+item);
				OpenWareInstance.getInstance()
						.logTrace("item10MinutesAgo = "+item10MinutesAgo);
				OpenWareInstance.getInstance()
						.logTrace("item20MinutesAgo = "+item20MinutesAgo);

				//item.streamPrint(System.out);
				//item10MinutesAgo.streamPrint(System.out);
				//item20MinutesAgo.streamPrint(System.out);

				OpenWareDataItem existingItem10MinutesAgo = DataService.getHistoricalSensorData(name, source, epochMilli-600000, epochMilli-600000);
				OpenWareDataItem existingItem20MinutesAgo = DataService.getHistoricalSensorData(name, source, epochMilli-1200000, epochMilli-1200000);

				OpenWareInstance.getInstance()
						.logTrace("existingIItem10MinutesAgo = "+existingItem10MinutesAgo);
				OpenWareInstance.getInstance()
						.logTrace("existingIItem20MinutesAgo = "+existingItem20MinutesAgo);

				List<OpenWareValue> values10MinutesAgo = existingItem10MinutesAgo.value();

				if(!values10MinutesAgo.isEmpty())
				{
					OpenWareValueDimension dim10MinutesAgo = values10MinutesAgo.get(0).get(0);
					double flow10MinutesAgo = ((OpenWareNumber) dim10MinutesAgo).value();

					if(flow10MinutesAgo == 0.0)
					{
						OpenWareInstance.getInstance()
								.logTrace("Zero was delivered 10 minutes ago ("+(epochMilli-600000));
						DataService.onNewData(item10MinutesAgo);
					}
					else
					{
						OpenWareInstance.getInstance()
								.logTrace("Some value was delivered 10 minutes ago, do not replace ("+(epochMilli-600000));
					}
				}
				else
				{
					OpenWareInstance.getInstance()
							.logTrace("No value was delivered 10 minutes ago ("+(epochMilli-600000));
					DataService.onNewData(item10MinutesAgo);
				}

				List<OpenWareValue> values20MinutesAgo = existingItem20MinutesAgo.value();

				if(!values20MinutesAgo.isEmpty())
				{
					OpenWareValueDimension dim20MinutesAgo = values20MinutesAgo.get(0).get(0);
					double flow20MinutesAgo = ((OpenWareNumber) dim20MinutesAgo).value();

					if(flow20MinutesAgo == 0.0)
					{
						OpenWareInstance.getInstance()
								.logTrace("Zero was delivered 20 minutes ago, replace");
						DataService.onNewData(item20MinutesAgo);
					}
					else
					{
						OpenWareInstance.getInstance()
								.logTrace("Some value was delivered 20 minutes ago, do not replace");
					}
				}
				else
				{
					OpenWareInstance.getInstance()
							.logTrace("No value was delivered 20 minutes ago");
					DataService.onNewData(item20MinutesAgo);
				}


				DataService.onNewData(item);

				OpenWareInstance.getInstance()
						.logTrace("Before HTTPResponseHelper.ok(ctx, item);");
				HTTPResponseHelper.ok(ctx, item);

			} catch (JSONException e) {
				OpenWareInstance.getInstance().logError("Malformed data posted to Keyence TTN Integration API\n" + ctx.body(),
						e);
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
