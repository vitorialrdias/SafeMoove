import requests
import time

ACCESS_TOKEN = "24da8085-fd38-32a3-9437-bffddb3f5cbd"

headers = {
    "Authorization": f"Bearer {ACCESS_TOKEN}",
    "Accept": "application/json"
}

# 1. Buscar linha
url_linha = "https://homolog.gateway.apilib.prefeitura.sp.gov.br/sptrans/olhovivo/v2.1/Linha/Buscar"

params = {"termosBusca": "8000"}

r = requests.get(url_linha, headers=headers, params=params)

if r.status_code != 200:
    print("Erro ao buscar linha:", r.text)
    exit()

linhas = r.json()

if not linhas:
    print("Nenhuma linha encontrada")
    exit()

codigo_linha = linhas[0]["cl"]

print("Linha encontrada:", linhas[0])
print("Código linha:", codigo_linha)

# 2. LOOP AO VIVO (POSIÇÃO)
url_posicao = "https://homolog.gateway.apilib.prefeitura.sp.gov.br/sptrans/olhovivo/v2.1/Posicao/Linha"

while True:
    r = requests.get(
        url_posicao,
        headers=headers,
        params={"codigoLinha": codigo_linha}
    )

    print("\nSTATUS:", r.status_code)

    if r.status_code != 200:
        print("Erro:", r.text)
        time.sleep(5)
        continue

    try:
        data = r.json()
    except Exception:
        print("Resposta inválida:", r.text)
        time.sleep(5)
        continue

    veiculos = data.get("vs", [])

    print(f"Ônibus ativos: {len(veiculos)}")

    for v in veiculos:
        print(f"Prefixo {v['p']} | lat={v['py']} lon={v['px']}")

    time.sleep(10)