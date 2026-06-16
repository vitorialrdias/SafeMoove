import requests

# Defina suas credenciais e parâmetros
TOKEN = "e184c9d51f42bc5aaea462f559c761cbd84c27cfa89028cf93a25dfdff77a965"

# LINK CORRIGIDO: Utilizando o subdomínio exclusivo do Olho Vivo
BASE_URL = "https://api.olhovivo.sptrans.com.br/v2.1"
LINHA_BUSCA = "8000"

# Cria a sessão HTTP para gerenciar os cookies de acesso
session = requests.Session()

# Cabeçalhos obrigatórios para requisições POST vazias
headers_auth = {
    "Content-Length": "0",
    "Accept": "*/*"
}

print("Iniciando autenticação...")
auth_url = f"{BASE_URL}/Login/Autenticar?token={TOKEN}"

try:
    # Realiza a chamada POST com os headers configurados
    response_auth = session.post(auth_url, headers=headers_auth)
    resultado = response_auth.status_code

    if response_auth.status_code == 200 and response_auth.text.lower() == "true":
        print("Autenticação realizada com sucesso!\n")
        
        # Passo 2: Buscar informações da linha
        print(f"Buscando informações da linha {LINHA_BUSCA}...")
        busca_url = f"{BASE_URL}/Linha/Buscar?termosBusca={LINHA_BUSCA}"
        response_linha = session.get(busca_url)
        dados_linha = response_linha.json()
        print("Resposta da API:")
        print(dados_linha)


        if not dados_linha:
            print(f"Nenhuma linha encontrada para o termo '{LINHA_BUSCA}'.")
        else:
            # Pega o primeiro resultado encontrado
            linha_selecionada = dados_linha[0] if isinstance(dados_linha, list) else dados_linha
            codigo_linha = linha_selecionada["cl"]
            print(f"Linha encontrada: {linha_selecionada['lt']}-{linha_selecionada['tl']}")
            print(f"Código identificador interno (cl): {codigo_linha}\n")
            
            # Passo 3: Buscar posição em tempo real
            print(f"Monitorando veículos da linha em tempo real...")
            posicao_url = f"{BASE_URL}/Posicao?codigoLinha={codigo_linha}"
            response_posicao = session.get(posicao_url)
            dados_posicao = response_posicao.json()
            
            veiculos = dados_posicao.get("vs", [])
            if not veiculos:
                print("Não há ônibus circulando nesta linha no momento.")
            else:
                print(f"Foram encontrados {len(veiculos)} ônibus ativos:")
                for v in veiculos:
                    print(f"- Prefixo: {v['p']} | Lat: {v['py']} | Lon: {v['px']}")
    else:
        print(f"Falha na autenticação. Código HTTP: {response_auth.status_code}")
        print(f"Resposta do servidor: {response_auth.text}")

except Exception as e:
    print(f"Ocorreu um erro na requisição: {e}")
