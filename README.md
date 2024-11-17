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
   - react app
