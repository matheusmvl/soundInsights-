-- Criação da camada gold

-- View para artistas com lançamentos recentes e sua popularidade média
USE CATALOG soundinsights;

CREATE OR REPLACE VIEW soundinsights.gold.agg_artistas_lancamentos_recentes AS
SELECT
    cfd.id_artista_principal_faixa AS id_artista,
    cfd.nome_artista_principal AS nome_artista,
    ROUND(AVG(cfd.popularidade_faixa)) AS media_popularidade_lancamentos_recentes,
    COUNT(cfd.id_faixa) AS total_lancamentos_recentes_90_dias
FROM
    soundinsights.gold.curated_faixa_detalhes AS cfd
WHERE
    -- Considera lançamentos dos últimos 90 dias
    DATEDIFF(CURRENT_DATE(), CAST(cfd.data_lancamento AS DATE)) <= 90
GROUP BY
    cfd.id_artista_principal_faixa,
    cfd.nome_artista_principal
ORDER BY
    media_popularidade_lancamentos_recentes DESC;


-- View para popularidade média de artistas por gênero
CREATE OR REPLACE VIEW soundinsights.gold.agg_popularidade_artista_por_genero AS
WITH exploded_genero AS (
    SELECT
        s.id_artista,
        s.popularidade,
        s.qt_seguidores,
        trim(genero_explode) AS genero_explode
    FROM
        soundinsights.silver.dim_artista AS s
        LATERAL VIEW explode(split(s.genero, ',')) AS genero_explode
    WHERE
        trim(genero_explode) IS NOT NULL
)
SELECT
    genero_explode AS genero,
    CEIL(AVG(popularidade)) AS media_popularidade_artista,
    COUNT(DISTINCT id_artista) AS total_artistas_no_genero,
    CEIL(SUM(qt_seguidores)) AS total_seguidores_genero
FROM
    exploded_genero
GROUP BY
    genero
ORDER BY
    media_popularidade_artista DESC;


-- View para detalhes enriquecidos de faixas com informações do artista
CREATE OR REPLACE VIEW soundinsights.gold.curated_faixa_detalhes AS
SELECT
    f.id_faixa,
    f.nome_faixa,
    f.popularidade AS popularidade_faixa,
    f.nome_album,
    f.data_lancamento,
    f.duracao_minutos,
    f.id_artista AS id_artista_principal_faixa, -- ID do artista da faixa
    a.nome_artista AS nome_artista_principal,
    a.genero AS generos_artista_principal, -- Gêneros do artista principal
    a.popularidade AS popularidade_artista_principal, -- Popularidade do artista principal
    a.qt_seguidores AS seguidores_artista_principal
FROM
    soundinsights.silver.dim_faixa AS f 
LEFT JOIN
    soundinsights.silver.dim_artista AS a ON f.id_artista = a.id_artista;

-- View para análise de popularidade e contagem de músicas por gênero em playlists
CREATE OR REPLACE VIEW soundinsights.gold.agg_playlists_genero_popularidade AS
SELECT
    p.genero,
    COUNT(p.nome_musica) AS total_musicas_por_genero_em_playlists,
    ROUND(AVG(p.popularidade)) AS media_popularidade_musicas_em_playlists
FROM
    soundinsights.silver.dim_playlist AS p
GROUP BY
    p.genero
ORDER BY
    media_popularidade_musicas_em_playlists DESC,
    total_musicas_por_genero_em_playlists DESC;
