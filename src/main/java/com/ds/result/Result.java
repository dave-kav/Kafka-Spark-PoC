package com.ds.result;

import java.io.Serializable;

public class Result implements Serializable {
	
	private static final long serialVersionUID = -1606774836008670847L;
	private String homeTeam;
	private String awayTeam;
	private int homeScore;
	private int awayScore;
	private String result = "";
	
	public Result(String homeTeam, int homeScore, String awayTeam, int awayScore) {
		this.homeTeam = homeTeam;
		this.homeScore = homeScore;
		this.awayTeam = awayTeam;
		this.awayScore = awayScore;
	}

	public String getHomeTeam() {
		return homeTeam;
	}

	public void setHomeTeam(String homeTeam) {
		this.homeTeam = homeTeam;
	}

	public String getAwayTeam() {
		return awayTeam;
	}

	public void setAwayTeam(String awayTeam) {
		this.awayTeam = awayTeam;
	}

	public int getHomeScore() {
		return homeScore;
	}

	public void setHomeScore(int homeScore) {
		this.homeScore = homeScore;
	}

	public int getAwayScore() {
		return awayScore;
	}

	public void setAwayScore(int awayScore) {
		this.awayScore = awayScore;
	}
	
	public String getResult() {
		return result;
	}

	public void setResult(String result) {
		this.result = result;
	}

	@Override
	public String toString() {
		return String.format("%s%s %d - %d %s", result, homeTeam, homeScore, awayScore, awayTeam);
	}

}
