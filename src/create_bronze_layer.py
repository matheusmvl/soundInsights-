# Databricks notebook source
# MAGIC %run ./utils/authentication

# COMMAND ----------

client_id = dbutils.secrets.get(scope="spotify-creds", key="client_id")
client_secret = dbutils.secrets.get(scope="spotify-creds", key="client_secret")

token = get_spotify_token(client_id, client_secret)

# COMMAND ----------

def get_playlist_items(token, playlist_id):
    headers = {
        "Authorization": f"Bearer {token}"
    }
    url = f"https://api.spotify.com/v1/playlists/{playlist_id}/tracks"
    response = requests.get(url, headers=headers)
    return response


# COMMAND ----------

def get_artists_from_playlist(playlist_id: str, limit_per_request: int = 100) -> list:
    """
    Obtém uma lista de artistas presentes em uma playlist específica do Spotify,
    juntamente com seus detalhes (incluindo popularidade e gêneros).

    Args:
        token (str): O token de acesso Bearer da API do Spotify.
        playlist_id (str): O ID da playlist do Spotify a ser consultada.
        limit_per_request (int): O número máximo de faixas a buscar por requisição (máx. 100).

    Returns:
        list: Uma lista de dicionários, onde cada dicionário representa um artista
              com seu ID, nome, popularidade e gêneros, ordenada por popularidade.
              Retorna uma lista vazia se a playlist não puder ser processada.
    """
    base_api_url = 'https://api.spotify.com/v1/' # URL base para os endpoints da API
    headers = {'Authorization': f'Bearer {token}'}

    all_artists_data = {} # Usar um dicionário para armazenar artistas únicos e seus detalhes
    offset = 0
    total_tracks = -1 # Inicializado para entrar no loop

    while total_tracks == -1 or offset < total_tracks:
        endpoint = f'playlists/{playlist_id}/tracks'
        url = base_api_url + endpoint
        params = {'limit': limit_per_request, 'offset': offset}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status() # Lança exceção para erros HTTP (4xx ou 5xx)
            tracks_page = response.json()
            tracks_data = tracks_page.get('items', [])
            
            if total_tracks == -1: # Define o total_tracks na primeira requisição
                total_tracks = tracks_page.get('total', 0)

            if not tracks_data: # Se não há mais faixas, sai do loop
                break

            for item in tracks_data:
                track = item.get('track')
                if track and track.get('artists'):
                    for artist_info in track.get('artists'):
                        artist_id = artist_info.get('id')
                        # Se o artista ainda não foi processado, busque seus detalhes completos
                        if artist_id and artist_id not in all_artists_data:
                            artist_details_endpoint = f'artists/{artist_id}'
                            artist_details_url = base_api_url + artist_details_endpoint
                            
                            artist_details_response = requests.get(artist_details_url, headers=headers)
                            if artist_details_response.status_code == 200:
                                details = artist_details_response.json()
                                all_artists_data[artist_id] = {
                                    'id': details.get('id'),
                                    'track_name': track['name'],
                                    'name': details.get('name'),
                                    'popularity': details.get('popularity'),
                                    'genres': details.get('genres', [])
                                }
                            else:
                                print(f"Erro ao buscar detalhes do artista {artist_id}: {artist_details_response.status_code} - {artist_details_response.text}")
            
            offset += limit_per_request # Incrementa o offset para a próxima página

        except requests.exceptions.RequestException as e:
            print(f"Erro ao buscar faixas da playlist {playlist_id}: {e}")
            break # Sai do loop em caso de erro na requisição

    # Converte o dicionário de artistas para uma lista e ordena por popularidade
    sorted_artists = sorted(list(all_artists_data.values()), key=lambda x: x.get('popularity', 0), reverse=True)
    
    return sorted_artists

# COMMAND ----------

def get_artist_details_by_name(token: str, artist_name: str):
    import requests
    base_api_url = 'https://api.spotify.com/v1/'
    headers = {'Authorization': f'Bearer {token}'}

    search_url = base_api_url + 'search'
    params = {
        'q': artist_name,
        'type': 'artist',
        'limit': 1
    }

    try:
        response = requests.get(search_url, headers=headers, params=params)
        response.raise_for_status()
        data = response.json()
        artists = data.get('artists', {}).get('items', [])
        if not artists:
            print(f"Nenhum artista encontrado para: {artist_name}")
            return None
        artist_id = artists[0].get('id')

        # Busca detalhes completos do artista usando o ID
        artist_details_url = base_api_url + f'artists/{artist_id}'
        artist_details_response = requests.get(artist_details_url, headers=headers)
        artist_details_response.raise_for_status()
        details = artist_details_response.json()

        # Converte lista de gêneros para string separada por vírgula
        genres_str = ', '.join(details.get('genres', []))

        # Monta dicionário com o resultado ajustado
        return {
            'id': details.get('id'),
            'name': details.get('name'),
            'popularity': details.get('popularity'),
            'genres': genres_str,
            'followers': details.get('followers', {}).get('total'),
            'url': details.get('external_urls', {}).get('spotify')
        }

    except requests.exceptions.RequestException as e:
        print(f"Erro ao buscar artista {artist_name}: {e}")
        return None


# COMMAND ----------

def get_artists_top_tracks(token: str, artist_ids: list, market: str = "BR") -> list:
    import requests
    """
    Busca as top tracks de uma lista de artistas no Spotify.

    Args:
        token (str): Token de autenticação da API Spotify.
        artist_ids (list): Lista de IDs de artistas.
        market (str): Código do mercado (ex: 'BR' para Brasil).

    Returns:
        list: Lista de dicionários com informações das top tracks de cada artista.
    """
    base_url = "https://api.spotify.com/v1/artists"
    headers = {"Authorization": f"Bearer {token}"}
    all_tracks = []

    for artist_id in artist_ids:
        url = f"{base_url}/{artist_id}/top-tracks"
        params = {"market": market}
        response = requests.get(url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            for track in data.get("tracks", []):
                track_info = {
                    "artist_id": artist_id,
                    "track_id": track.get("id"),
                    "track_name": track.get("name"),
                    "popularity": str(track.get("popularity")),
                    "album_name": track.get("album", {}).get("name"),
                    "release_date": track.get("album", {}).get("release_date"),
                    "preview_url": track.get("preview_url"),
                    "duration_ms": str(track.get("duration_ms")),
                    "external_url": track.get("external_urls", {}).get("spotify")
                }
                all_tracks.append(track_info)
        else:
            print(f"Erro ao buscar top tracks para {artist_id}: {response.status_code} - {response.text}")

    return all_tracks


# COMMAND ----------

# playlist mais tocadas no brasil = 1AM1a7HSW9shTU7B1p2a1B
# playlist de teste = 37i9dQZF1DX0FOF1IUWK1W

lista = get_artists_from_playlist('1AM1a7HSW9shTU7B1p2a1B')
df_lista = spark.createDataFrame(lista)

from pyspark.sql.functions import col, concat_ws
df_lista = df_lista.withColumn('genres', concat_ws(',', col('genres')))
display(df_lista)

# COMMAND ----------

# Obter lista distinta de nomes de artistas do campo "name" do df_lista
artist_names = [row.name for row in df_lista.select("name").distinct().collect()]

artist_details_list = []

for name in artist_names:
    details = get_artist_details_by_name(token, name)
    if details:
        artist_details_list.append(details)

df_artist_details = spark.createDataFrame(artist_details_list)
df_artist_details = df_artist_details.select(
    'id', 'name', 'genres', 'popularity', 'followers', 'url'
)

display(df_artist_details)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType

artist_ids = [row.id for row in df_artist_details.select("id").collect()]
top_tracks = get_artists_top_tracks(token, artist_ids)

# Schema explícito
schema = StructType([
    StructField("artist_id", StringType(), True),
    StructField("track_id", StringType(), True),
    StructField("track_name", StringType(), True),
    StructField("popularity", StringType(), True),
    StructField("album_name", StringType(), True),
    StructField("release_date", StringType(), True),
    StructField("preview_url", StringType(), True),
    StructField("duration_ms", StringType(), True),
    StructField("external_url", StringType(), True)
])

# Criar DataFrame com schema
df_top_tracks = spark.createDataFrame(top_tracks, schema=schema)
df_top_tracks = df_top_tracks.select(
    "artist_id", "track_id", "track_name", "popularity",
    "album_name", "release_date", "preview_url",
    "duration_ms", "external_url"
)

display(df_top_tracks)

# COMMAND ----------

df_top_tracks  .printSchema()

# COMMAND ----------

from pyspark.sql.functions import col

# Função para converter todas as colunas de um DataFrame para string
def convert_columns_to_string(df):
    columns = df.columns
    for column in columns:
        df = df.withColumn(column, col(column).cast("string"))
    return df

# Converter colunas de df_lista para string
df_lista = convert_columns_to_string(df_lista)

# Converter colunas de df_artist_details para string
df_artist_details = convert_columns_to_string(df_artist_details)

# Converter colunas de df_top_tracks para string
df_top_tracks = convert_columns_to_string(df_top_tracks)

# Exibir os DataFrames
df_lista.printSchema()
df_artist_details.printSchema()
df_top_tracks.printSchema()

# COMMAND ----------

# Salvar df_lista como tabela Delta
df_lista.write.format("delta").mode("overwrite").saveAsTable("soundinsights.bronze.df_lista")

# Salvar df_artist_details como tabela Delta
df_artist_details.write.format("delta").mode("overwrite").saveAsTable("soundinsights.bronze.df_artist_details")

# Salvar df_top_tracks como tabela Delta
df_top_tracks.write.format("delta").mode("overwrite").saveAsTable("soundinsights.bronze.df_top_tracks")