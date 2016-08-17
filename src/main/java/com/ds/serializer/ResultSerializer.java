package com.ds.serializer;

import java.lang.reflect.Type;

import com.ds.result.Result;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializationContext;
import com.google.gson.JsonSerializer;

public class ResultSerializer implements JsonSerializer<Result> {

	@Override
	public JsonElement serialize(Result src, Type typeOfSrc, JsonSerializationContext context) {
		 JsonObject result = new JsonObject();
	        result.add("homeTeam", new JsonPrimitive(src.getHomeTeam()));
	        result.add("homeScore", new JsonPrimitive(src.getHomeScore()));
	        result.add("awayTeam", new JsonPrimitive(src.getAwayTeam()));
	        result.add("awayScore", new JsonPrimitive(src.getAwayScore()));

	        return result;
	}
}