# Databricks notebook source
import requests

def get_audio_features_for_tracks(token: str, track_ids: list) -> list:
    """
    Busca as audio features para uma lista de tracks do Spotify.

    Args:
        token (str): Token de autenticação da API do Spotify.
        track_ids (list): Lista de track IDs (máximo 100 por requisição).

    Returns:
        list: Lista de dicionários com os campos das audio features.
    """
    base_url = "https://api.spotify.com/v1/audio-features"
    headers = {"Authorization": f"Bearer {token}"}
    audio_features_list = []

    # A API permite no máximo 100 track IDs por chamada
    for i in range(0, len(track_ids), 100):
        batch_ids = track_ids[i:i+100]
        params = {"ids": ",".join(batch_ids)}
        response = requests.get(base_url, headers=headers, params=params)

        if response.status_code == 200:
            data = response.json()
            audio_features = data.get("audio_features", [])
            for af in audio_features:
                if af:  # pode haver null
                    audio_features_list.append({
                        "track_id": af.get("id"),
                        "danceability": af.get("danceability"),
                        "energy": af.get("energy"),
                        "key": af.get("key"),
                        "loudness": af.get("loudness"),
                        "mode": af.get("mode"),
                        "speechiness": af.get("speechiness"),
                        "acousticness": af.get("acousticness"),
                        "instrumentalness": af.get("instrumentalness"),
                        "liveness": af.get("liveness"),
                        "valence": af.get("valence"),
                        "tempo": af.get("tempo"),
                        "duration_ms": af.get("duration_ms"),
                        "time_signature": af.get("time_signature")
                    })
        else:
            print(f"Erro ao buscar audio features: {response.status_code} - {response.text}")

    return audio_features_list


# COMMAND ----------

# teste de acesso: tentativa de correção erro 403
display(get_audio_features_by_track_id(token,"4PiA2gsPjf5jD1zlv1DeyH"))