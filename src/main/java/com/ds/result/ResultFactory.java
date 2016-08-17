package com.ds.result;

import java.util.List;
import java.util.Random;

public class ResultFactory {

	public static Result generateResult(List<String> teams) {

		Random random = new Random();
		
		String homeTeam = teams.get(random.nextInt(teams.size() - 1));
		int homeScore = random.nextInt(8);
		
		String awayTeam = homeTeam;
		
		//make sure teams not same
		while (homeTeam.equals(awayTeam))
			awayTeam = teams.get(random.nextInt(teams.size() - 1));
		int awayScore  = random.nextInt(5);

		return new Result(homeTeam, homeScore, awayTeam, awayScore);
	}
}
