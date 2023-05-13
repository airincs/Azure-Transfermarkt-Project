# Databricks notebook source
# MAGIC %md
# MAGIC ##Reading in CSVs from the raw container, transforming the data, then writing to a new clean container

# COMMAND ----------

secret = dbutils.secrets.get(scope = 'transfermarktsecretscope', key = 'datalake-access-key')
spark.conf.set(
    "fs.azure.account.key.transfermarktdata23dl.dfs.core.windows.net",
    secret
)

# COMMAND ----------

appearances_df = spark.read.option("header", "true").option("inferSchema", "true").csv("abfss://raw@transfermarktdata23dl.dfs.core.windows.net/appearances.csv")
club_games_df = spark.read.option("header", "true").option("inferSchema", "true").csv("abfss://raw@transfermarktdata23dl.dfs.core.windows.net/club_games.csv")
clubs_df = spark.read.option("header", "true").option("inferSchema", "true").csv("abfss://raw@transfermarktdata23dl.dfs.core.windows.net/clubs.csv")
competitions_df = spark.read.option("header", "true").option("inferSchema", "true").csv("abfss://raw@transfermarktdata23dl.dfs.core.windows.net/competitions.csv")
game_events_df = spark.read.option("header", "true").option("inferSchema", "true").csv("abfss://raw@transfermarktdata23dl.dfs.core.windows.net/game_events.csv")
players_df = spark.read.option("header", "true").option("inferSchema", "true").csv("abfss://raw@transfermarktdata23dl.dfs.core.windows.net/players.csv")
games_df = spark.read.option("header", "true").option("inferSchema", "true").csv("abfss://raw@transfermarktdata23dl.dfs.core.windows.net/games.csv")
player_valuations_df = spark.read.option("header", "true").option("inferSchema", "true").csv("abfss://raw@transfermarktdata23dl.dfs.core.windows.net/player_valuations.csv")

# COMMAND ----------

display(appearances_df)

# COMMAND ----------

games_master_df = competitions_df.join(games_df, competitions_df.competition_id == games_df.competition_id) \
    .select(competitions_df.name, competitions_df.type, competitions_df.sub_type, competitions_df.country_name, competitions_df.country_latitude, competitions_df.country_longitude, competitions_df.confederation, games_df.game_id, games_df.season, \
        games_df.round, games_df.date, games_df.home_club_id, games_df.away_club_id, games_df.home_club_goals, games_df.away_club_goals, games_df.home_club_position, games_df.away_club_position, \
        games_df.club_home_name, games_df.club_away_name, games_df.attendance, games_df.referee)

# COMMAND ----------

display(games_master_df)

# COMMAND ----------

appearances_df.write.mode("overwrite").option("header","true").csv("abfss://clean@transfermarktdata23dl.dfs.core.windows.net/appearances")
club_games_df.write.mode("overwrite").option("header","true").csv("abfss://clean@transfermarktdata23dl.dfs.core.windows.net/club_games")
clubs_df.write.mode("overwrite").option("header","true").csv("abfss://clean@transfermarktdata23dl.dfs.core.windows.net/clubs")
game_events_df.write.mode("overwrite").option("header","true").csv("abfss://clean@transfermarktdata23dl.dfs.core.windows.net/game_events")
player_valuations_df.write.mode("overwrite").option("header","true").csv("abfss://clean@transfermarktdata23dl.dfs.core.windows.net/player_valuations")
games_master_df.write.mode("overwrite").option("header","true").csv("abfss://clean@transfermarktdata23dl.dfs.core.windows.net/games_master")
players_df.write.mode("overwrite").option("header","true").csv("abfss://clean@transfermarktdata23dl.dfs.core.windows.net/players")

# COMMAND ----------

display(players_df.printSchema())

# COMMAND ----------

display(appearances_df)
