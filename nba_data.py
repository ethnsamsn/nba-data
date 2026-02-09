import pandas as pd
import requests


def create_season_games():

    request_url = 'https://stats.nba.com/stats/leaguegamelog?Counter=1000&DateFrom=&DateTo=&Direction=DESC&ISTRound=&LeagueID=00&PlayerOrTeam=T&Season=2025-26&SeasonType=Regular%20Season&Sorter=DATE' 


    headers =   {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "Accept": "application/json",
    }

    r = requests.get(request_url, headers=headers).json()

    table_headers = r["resultSets"][0]["headers"]

    cols = ["VIDEO_AVAILABLE"]
    df = pd.DataFrame(r["resultSets"][0]["rowSet"], columns=table_headers).drop(cols, axis=1)

    return df

def validate_two_rows_per_game(df):
    counts = df.groupby("GAME_ID").size()

    invalid = counts[counts != 2]

    if not invalid.empty:
        raise ValueError(
            f"Found games with invalid row counts (expected 2):\n{invalid}"
        )

    return df

def pivot_games_to_single_row(df):
    grouped = df.groupby("GAME_ID")
    away_games = grouped.nth(0).reset_index(drop=True)
    home_games = grouped.nth(1).reset_index(drop=True)
    keep_cols = ["GAME_ID", "GAME_DATE_EST"]
    away_rename = {col: f"away_{col}" for col in away_games.columns if col not in keep_cols}
    home_rename = {col: f"home_{col}" for col in home_games.columns if col not in keep_cols}
    
    away_games = away_games.rename(columns=away_rename)
    home_games = home_games.rename(columns=home_rename)
    away_games = away_games.set_index("GAME_ID")
    home_games = home_games.set_index("GAME_ID")
    result = pd.concat([away_games, home_games], axis=1).reset_index()
    result['MATCHUP_TITLE'] = result['away_TEAM_ABBREVIATION'] + ' vs ' + result['home_TEAM_ABBREVIATION']
    
    return result



if __name__ == "__main__":

    df = create_season_games()

    validate_two_rows_per_game(df)

    df = pivot_games_to_single_row(df)

    df.to_csv('nba_games.csv', index=False)
    print("Data saved to nba_games.csv")
    print(df.head(10))
    




