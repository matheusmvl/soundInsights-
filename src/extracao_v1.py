# variaveis de configuração (mascaradas)
client_id = '****************************'
client_secret = '**************************'

base_url = 'https://api.spotify.com/v1/' # URL base para os endpoints da API

# Token e autenticação 
import requests
from base64 import b64encode

auth_url = 'https://accounts.spotify.com/api/token'
auth_headers = {
    'Authorization': 'Basic ' + b64encode(f'{client_id}:{client_secret}'.encode()).decode('utf-8')
}
auth_data = {
    'grant_type': 'client_credentials'
}

response = requests.post(auth_url, headers=auth_headers, data=auth_data)

if response.status_code == 200:
    token = response.json().get('access_token')


# função de teste dados

def get_artist_by_id(artist_id):
    endpoint = f'artists/{artist_id}/top-tracks'
    url = base_url + endpoint
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"Erro ao buscar artista por ID {artist_id}: {response.status_code} - {response.text}")
        return None


pitbull_id = '0TnOYISbd1XYRBk9myaseg' # Exemplo de ID
artist_data = get_artist_by_id(pitbull_id)

if artist_data:
    print(json.dumps(artist_data, indent=4))
    print(f"\nNome do artista: {artist_data.get('name')}")
    print(f"Popularidade: {artist_data.get('popularity')}")
    print(f"Gêneros: {', '.join(artist_data.get('genres', []))}")
else:
    print("Não foi possível obter dados do artista.")

# BACKLOG: func procurar artistas?

# --- 3. FUNÇÃO PARA BUSCAR ARTISTA POR NOME E RETORNAR O ID ---
def search_artist_id(artist_name):
    endpoint = 'search'
    params = {
        'q': artist_name,
        'type': 'artist',
        'limit': 1 # Queremos o primeiro e mais relevante resultado
    }
    url = base_url + endpoint
    response = requests.get(url, headers=headers, params=params)

    if response.status_code == 200:
        data = response.json()
        artists = data.get('artists', {}).get('items')
        if artists:
            # Retorna o ID do primeiro artista encontrado
            return artists[0].get('id')
        else:
            print(f"Nenhum artista encontrado com o nome '{artist_name}'.")
            return None
    else:
        print(f"Erro ao buscar artista '{artist_name}': {response.status_code} - {response.text}")
        return None

## Teste de artista
artist_to_find = "Taylor Swift"
taylor_swift_id = search_artist_id(artist_to_find)

if taylor_swift_id:
    print(f"O ID do artista '{artist_to_find}' é: {taylor_swift_id}")
    # Agora você pode usar este ID para obter mais detalhes do artista
    # Por exemplo, usando a função get_artist_by_id da resposta anterior
    # artist_details = get_artist_by_id(taylor_swift_id)
    # if artist_details:
    #     print(json.dumps(artist_details, indent=4))
else:
    print(f"Não foi possível encontrar o ID para '{artist_to_find}'.")


## Fuc principal artistas por playlist ##

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



#playlist mais tocadas no brasil = 1AM1a7HSW9shTU7B1p2a1B (recolhida manualmente)
lista = get_artists_from_playlist('1AM1a7HSW9shTU7B1p2a1B')
df_lista = spark.createDataFrame(lista)

from pyspark.sql.functions import col, concat_ws
df_lista = df_lista.withColumn('genres', concat_ws(',', col('genres')))

output_path = 'abfss://<masked_url>/api/spotify/temp'
df_lista.coalesce(1).write.csv(output_path, header=True, mode='overwrite')


# move e renomea os arquivos csv da pasta temp para pasta do projeto

import os
from datetime import datetime

current_date = datetime.now().strftime('%Y-%m-%d')
destination_path = 'abfss://<masked_url>/api/spotify/'# Lista os arquivos CSV na pasta de saída

csv_files = dbutils.fs.ls(output_path)
csv_files = [file.path for file in csv_files if file.path.endswith('.csv')]

# Move e renomeia os arquivos CSV
for file in csv_files:
    new_file_name = f'file_{current_date}.csv'
    new_file_path = os.path.join(destination_path, new_file_name)
    dbutils.fs.mv(file, new_file_path)

print(f"Arquivos movidos e renomeados para {destination_path} com sucesso.")