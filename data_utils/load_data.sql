-- create tables
--
CREATE TABLE dbo.payments (
	event_id INT,
	player_id NVARCHAR(5),
	ts DATETIME2,
	amount FLOAT,
	transactions INT
)

CREATE TABLE dbo.stats (
	event_id INT,
	player_id NVARCHAR(5),
	ts DATETIME2,
	win_loss_ratio FLOAT,
	games_played INT,
	time_in_game FLOAT
)


-- copy data into tables
--
COPY INTO dbo.payments
FROM 'https://dlssy7f5j76ur5d3pm.dfs.core.windows.net/dlssy7f5j76ur5d3pmfs1/payments.csv'
WITH
(
    FILE_TYPE = 'CSV'
    ,MAXERRORS = 0
    ,FIRSTROW = 2
)

COPY INTO dbo.stats
FROM 'https://dlssy7f5j76ur5d3pm.dfs.core.windows.net/dlssy7f5j76ur5d3pmfs1/stats.csv'
WITH
(
    FILE_TYPE = 'CSV'
    ,MAXERRORS = 0
    ,FIRSTROW = 2
)