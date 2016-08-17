package com.ds.serializer;

import java.lang.reflect.Type;

import com.ds.result.Result;
import com.google.gson.JsonDeserializationContext;
import com.google.gson.JsonDeserializer;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;

public class ResultDeserializer implements JsonDeserializer<Result> {

	@Override
	public Result deserialize(JsonElement json, Type typeOfT, JsonDeserializationContext context)
			throws JsonParseException {
		
		final JsonObject jsonObject = json.getAsJsonObject();
		String homeTeam = jsonObject.get("homeTeam").getAsString();
		int homeScore = jsonObject.get("homeScore").getAsInt();
		String awayTeam = jsonObject.get("awayTeam").getAsString();
		int awayScore = jsonObject.get("awayScore").getAsInt();
		
		return new Result(homeTeam, homeScore, awayTeam, awayScore);
	}
}