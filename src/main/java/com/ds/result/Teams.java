package com.ds.result;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

public class Teams {

	public static List<String> getTeams() throws IOException {

		//connect to source
		final String sURL = "https://raw.githubusercontent.com/openfootball/football.json/master/2015-16/en.1.clubs.json";
		URL url = null;
		url = new URL(sURL);
		HttpURLConnection request = null;
		request = (HttpURLConnection) url.openConnection();
		request.connect();

		//parse json
		JsonParser jp = new JsonParser();
		JsonElement root = null;
		root = jp.parse(new InputStreamReader((InputStream) request.getContent()));
		JsonObject rootobj = root.getAsJsonObject();
		JsonArray jsonArray = rootobj.getAsJsonArray("clubs");

		//add team names to element list
		List<String> teams = new ArrayList<>();
		
		for (JsonElement element: jsonArray) {
			//TODO debug below line, not working
			String teamName = element.getAsJsonObject().get("name").getAsString(); 
			teams.add(teamName);
		}
		
		return teams;
	}
}
