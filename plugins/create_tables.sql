-- SQL commands for creating tables

-- create teams table sql
CREATE TABLE IF NOT EXISTS teams (
    team_id INT PRIMARY KEY,
    team_name VARCHAR(100),
    short_name VARCHAR(100)
);


-- create matches table sql 
CREATE TABLE IF NOT EXISTS matches (
    match_id SERIAL PRIMARY KEY,
    season VARCHAR(10),
    round VARCHAR(5) NOT NULL,
    home_team_id INT REFERENCES teams(team_id),
    away_team_id INT REFERENCES teams(team_id),
    score VARCHAR(10),
    time TIMESTAMP
);


-- create standings table sql
CREATE TABLE IF NOT EXISTS standings (
    standing_id SERIAL PRIMARY KEY,
    season VARCHAR(10) NOT NULL,
    team_id INT REFERENCES teams(team_id),
    rank INT NOT NULL,
    played INT NOT NULL,
    wins INT NOT NULL,
    draws INT NOT NULL,
    losses INT NOT NULL,
    pts INT NOT NULL,
    scoreStr VARCHAR(10),
    goalConDiff INT
);
