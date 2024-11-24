<script lang="ts">
    import { onMount } from "svelte";

    type Round = {
        id: number;
        game_id: string;
        round_number: number;
        player1_guess: string;
        player1_guess_country: string;
        player1_guess_region: string;
        player1_guess_state: string;
        player1_guess_city: string;
        player2_guess: string;
        player2_guess_country: string;
        player2_guess_region: string;
        player2_guess_state: string;
        player2_guess_city: string;
        correct_location: string;
        correct_location_country: string;
        correct_location_region: string;
        correct_location_state: string;
        correct_location_city: string;
        country_code: string;
        opponent_score: number;
        team_score: number;
        heading: number;
        pitch: number;
        zoom: number;
    };

    let rounds: Round[] = [];
    let selectedCountry: string = "";
    let countries: string[] = [];
    let averageScoreDiff: number = 0;

    onMount(async () => {
        try {
            const response = await fetch(
                "http://localhost:5000/api/team-duel-rounds",
            );
            rounds = await response.json();

            // Get unique countries from correct_location_country
            countries = [
                ...new Set(rounds.map((r) => r.correct_location_country)),
            ]
                .filter((country) => country) // Remove null/undefined
                .sort();
        } catch (error) {
            console.error("Error fetching data:", error);
        }
    });
</script>

<div class="container">
    <h1>Team Duel Analysis</h1>

    <div class="filter-section">
        <label for="country-select">Filter by Country:</label>
        <select id="country-select" bind:value={selectedCountry}>
            <option value="">All Countries</option>
            {#each countries as country}
                <option value={country}>{country}</option>
            {/each}
        </select>
    </div>

    <div class="stats-section">
        <h2>Statistics</h2>
        <p>
            Average Score Difference: {Math.round(
                selectedCountry
                    ? rounds
                          .filter(
                              (r) =>
                                  r.correct_location_country ===
                                  selectedCountry,
                          )
                          .reduce(
                              (sum, round) =>
                                  sum +
                                  (round.team_score - round.opponent_score),
                              0,
                          ) /
                          rounds.filter(
                              (r) =>
                                  r.correct_location_country ===
                                  selectedCountry,
                          ).length
                    : rounds.reduce(
                          (sum, round) =>
                              sum + (round.team_score - round.opponent_score),
                          0,
                      ) / rounds.length,
            )}
        </p>
        <p>
            Accuracy 1: {(selectedCountry
                ? (rounds.filter(
                      (r) =>
                          r.correct_location_country === selectedCountry &&
                          (r.player1_guess_country === selectedCountry ||
                              r.player2_guess_country === selectedCountry),
                  ).length /
                      rounds.filter(
                          (r) => r.correct_location_country === selectedCountry,
                      ).length) *
                  100
                : (rounds.filter(
                      (r) =>
                          r.player1_guess_country ===
                              r.correct_location_country ||
                          r.player2_guess_country ===
                              r.correct_location_country,
                  ).length /
                      rounds.length) *
                  100
            ).toFixed(2)}%
        </p>
        <p>
            Accuracy 2: {(selectedCountry
                ? ((rounds.filter(
                      (r) =>
                          r.correct_location_country === selectedCountry &&
                          r.player1_guess_country === selectedCountry,
                  ).length +
                      rounds.filter(
                          (r) =>
                              r.correct_location_country === selectedCountry &&
                              r.player2_guess_country === selectedCountry,
                      ).length) /
                      (rounds.filter(
                          (r) => r.correct_location_country === selectedCountry,
                      ).length *
                          2)) *
                  100
                : ((rounds.filter(
                      (r) =>
                          r.correct_location_country ===
                          r.player2_guess_country,
                  ).length +
                      rounds.filter(
                          (r) =>
                              r.correct_location_country ===
                              r.player1_guess_country,
                      ).length) /
                      (rounds.length * 2)) *
                  100
            ).toFixed(2)}%
        </p>
        <p>
            Number of Rounds: {selectedCountry
                ? rounds.filter(
                      (r) => r.correct_location_country === selectedCountry,
                  ).length
                : rounds.length}
        </p>
    </div>
</div>

<style>
    .container {
        max-width: 800px;
        margin: 0 auto;
        padding: 20px;
    }

    .filter-section {
        margin: 20px 0;
    }

    select {
        margin-left: 10px;
        padding: 5px;
        font-size: 16px;
    }

    .stats-section {
        background-color: #f5f5f5;
        padding: 20px;
        border-radius: 8px;
    }

    .positive {
        color: green;
        font-weight: bold;
    }

    .negative {
        color: red;
        font-weight: bold;
    }
</style>
