# Geoguessr Analyzer

## Workflow

We will work as follows: create issues here in GitHub, create a new branch locally, and then develop code in that branch that addresses the issue. After finishing development on a specific issue, push the code to GitHub and open a pull request.

### Helpful links
 - Learn Git [here](https://www.w3schools.com/git/default.asp?remote=github).
 - Learn more about issues [here](https://docs.github.com/en/issues/tracking-your-work-with-issues/about-issues)

## Description

This project aims to create an application for analyzing geoguessr games. The solution will consist of a data pipeline to extract and augment game data and an interface for interacting with and viewing the data (dashboard). 

## High level Tasks
- Set up data pipeline to get games
   - Extract all game IDs (GeoGuessr API)
   - Extract all game data (GeoGuessr API)
   - Find Country and region for guesses (GeoCode)
- Create application
   - sqlite database
   - svelte app
 
## General thoughts

Script for game data extraction, that formats data for web applicaiton. 

Web application prompts upload and saves to user. Allow for no signup then view our data. 

### Web application 
 - (easy) All, continent and country views
     - All: opponent diff, continent summary in opponent diff. Top 5 most common countries opponent diff score.
     - Continent: top 5 best countries, top 5 worst countries opponent diff. Country acc, avg score, avg opponent diff
     - Country: region acc (if applicable), country acc, #rounds, top 5 mistaken countries, score distribution. Top 3 worst regions.
 - (hard)
     - World map where you can highlight most common mistaken countries when hovering a country (red->yellow) red most guessed wrong
  
add on extension which does things for you skipping the copying of cookie
       
### Data pipeline
5s per round for team duels (1 API call per second 5 long lat per round)
3s per round for duels
2s per round for single player game

First iteration: batch processing
Second iteration: get only new games by looking at timestamps
