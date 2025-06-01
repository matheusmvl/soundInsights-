# Databricks notebook source
# tratamento bronze df_lista
from pyspark.sql.functions import col, trim

df_playlist = spark.read.table("soundinsights.bronze.df_lista")

# Remover duplicatas
df_playlist = df_playlist.dropDuplicates()

# Remover espaços em branco extras
df_playlist = df_playlist.withColumn("id", trim(col("id"))) \
                         .withColumn("name", trim(col("name"))) \
                         .withColumn("track_name", trim(col("track_name"))) \
                         .withColumn("genres", trim(col("genres")))

# Remover valores nulos
df_playlist = df_playlist.dropna()

display(df_playlist)

# COMMAND ----------

# cria a silver dim_playlist
create_dim_playlist = f"""
CREATE OR REPLACE TABLE soundinsights.silver.dim_playlist
USING delta
AS SELECT
    coalesce(id, 'unknown_id') AS id_artista,
    coalesce(name, 'unknown_name') AS nome_artista,
    coalesce(track_name, 'unknown_track') AS nome_musica,
    coalesce(genres, 'unknown_genre') AS genero,
    coalesce(CAST(popularity AS INT), 0) AS popularidade
FROM
    soundinsights.bronze.df_lista
"""
spark.sql(create_dim_playlist)

# COMMAND ----------

# tratamento bronze df_artist_details
from pyspark.sql.functions import col, trim

df_artist_details = spark.read.table("soundinsights.bronze.df_artist_details")

# Remover duplicatas
df_artist_details = df_artist_details.dropDuplicates()

# Remover espaços em branco extras
df_artist_details = df_artist_details.withColumn("id", trim(col("id"))) \
                                     .withColumn("name", trim(col("name"))) \
                                     .withColumn("genres", trim(col("genres"))) \
                                     .withColumn("popularity", trim(col("popularity")))

# Remover valores nulos
df_artist_details = df_artist_details.dropna()

display(df_artist_details)

# COMMAND ----------

# cria a silver dim_artista
create_dim_artista = f"""
CREATE OR REPLACE TABLE soundinsights.silver.dim_artista
USING delta
AS SELECT
    coalesce(id, 'unknown_id') AS id_artista,
    coalesce(name, 'unknown_name') AS nome_artista,
    coalesce(genres, 'unknown_genre') AS genero,
    coalesce(CAST(popularity AS INT), 0) AS popularidade,
    coalesce(regexp_replace(format_number(CAST(followers AS DOUBLE), 0), ',', '.'), '0') AS qt_seguidores,
    coalesce(url, 'unknown_url') AS url_spotify
FROM
    soundinsights.bronze.df_artist_details
"""
spark.sql(create_dim_artista)

# COMMAND ----------

# tratamento bronze df_top_tracks
from pyspark.sql.functions import col, trim

df_top_tracks = spark.read.table("soundinsights.bronze.df_top_tracks")

# Remover duplicatas
df_top_tracks = df_top_tracks.dropDuplicates()

# Remover espaços em branco extras
df_top_tracks = df_top_tracks.withColumn("artist_id", trim(col("artist_id"))) \
                             .withColumn("track_id", trim(col("track_id"))) \
                             .withColumn("track_name", trim(col("track_name"))) \
                             .withColumn("popularity", trim(col("popularity"))) \
                             .withColumn("album_name", trim(col("album_name"))) \
                             .withColumn("release_date", trim(col("release_date"))) \
                             .withColumn("preview_url", trim(col("preview_url"))) \
                             .withColumn("duration_ms", trim(col("duration_ms"))) \
                             .withColumn("external_url", trim(col("external_url")))

# Preencher valores nulos em preview_url com um valor padrão
df_top_tracks = df_top_tracks.fillna({'preview_url': 'unknown_preview_url'})

# Remover valores nulos restantes
df_top_tracks = df_top_tracks.dropna()

display(df_top_tracks)

# COMMAND ----------

df_top_tracks.columns

# COMMAND ----------

# cria a silver dim_faixa
create_dim_faixa = f"""
CREATE OR REPLACE TABLE soundinsights.silver.dim_faixa
USING delta
AS SELECT
    coalesce(artist_id, 'unknown_id') AS id_artista,
    coalesce(track_id, 'unknown_id') AS id_faixa,
    coalesce(track_name, 'unknown_track') AS nome_faixa,
    coalesce(CAST(popularity AS INT), 0) AS popularidade,
    coalesce(album_name, 'unknown_album') AS nome_album,
    coalesce(to_date(release_date, 'yyyy-MM-dd'), to_date('1900-01-01')) AS data_lancamento,
    coalesce(
        CONCAT(
            CAST(FLOOR(duration_ms / 60000) AS STRING), 
            ':', 
            LPAD(CAST(FLOOR((duration_ms % 60000) / 1000) AS STRING), 2, '0')
        ), 
        '0:00'
    ) AS duracao_minutos,
    coalesce(external_url, 'unknown_url') AS url_spotify
FROM
    soundinsights.bronze.df_top_tracks
"""
spark.sql(create_dim_faixa)