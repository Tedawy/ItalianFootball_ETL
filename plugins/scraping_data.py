import requests
import pandas as pd
from datetime import datetime

# Fetch data from the API
def fetch_data(season):
    params = {
        'id': '55',
        'season': season
    }
    response = requests.get('https://www.fotmob.com/api/leagues', params=params)
    data = response.json()
    matches = data['matches']['allMatches']
    standings = data['table'][0]['data']['table']['all']
    return matches, standings

# Process match data
def process_matches(matches, season):
    all_matches_data = []
    for week in matches:
        match_info = {
            'season': season,
            'round': week['round'],
            'home_team_id': week['home']['id'],
            'away_team_id': week['away']['id'],
            'score': week['status'].get('scoreStr', 'N/A'),
            'time': week['status']['utcTime']
        }
        all_matches_data.append(match_info)
    return all_matches_data

# Process standing data
def process_standings(standings, season):
    tables_data = []
    for team in standings:
        standing_info = {
            'season': season,
            'rank': team['idx'],
            'team_id': team['id'],
            'team_name': team['name'],
            'short_name': team['shortName'],
            'played': team['played'],
            'wins': team['wins'],
            'draws': team['draws'],
            'losses': team['losses'],
            'pts': team['pts'],
            'scoreStr': team['scoresStr'],
            'goalConDiff': team['goalConDiff']
        }
        tables_data.append(standing_info)
    return tables_data

# Generate SQL insert commands from data
def generate_insert_sql(table_name, data, columns):
    # Construct the column and placeholder strings
    columns_str = ", ".join(columns)
    placeholders = ", ".join(["%s"] * len(columns)) # (%s ,%s)
    
    # Create the SQL INSERT statement
    sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    # Extract values from data
    values = [tuple(row[col] for col in columns) for row in data]
    
    return sql, values

