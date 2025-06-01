# Databricks notebook source
import requests
from base64 import b64encode

def get_spotify_token(client_id, client_secret):
    auth_url = 'https://accounts.spotify.com/api/token'
    auth_headers = {
        'Authorization': 'Basic ' + b64encode(f'{client_id}:{client_secret}'.encode()).decode('utf-8')
    }
    auth_data = {
        'grant_type': 'client_credentials'
    }

    response = requests.post(auth_url, headers=auth_headers, data=auth_data)

    if response.status_code == 200:
        return response.json().get('access_token')
    else:
        raise Exception(f'Erro ao obter token: {response.status_code} - {response.text}')
