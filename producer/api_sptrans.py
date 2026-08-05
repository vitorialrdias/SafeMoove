import requests


class SPTransAPI:

    def __init__(self, token):
        self.token = token
        self.base_url = "http://api.olhovivo.sptrans.com.br/v2.1"
        self.session = requests.Session()
        self.autenticar()

    def autenticar(self):
        """Autentica na API e armazena os cookies na sessão."""
        url = f"{self.base_url}/Login/Autenticar"

        # Passa data={} para evitar erro '411 Length Required'
        response = self.session.post(url, params={"token": self.token}, data={})

        if response.status_code != 200 or response.text.strip().lower() != "true":
            print(f"Falha na autenticação SPTrans: status {response.status_code} - {response.text}")
            return False

        return True

    def obter_codigo_linha(self, linha):
        url = f"{self.base_url}/Linha/Buscar"
        r = self.session.get(url, params={"termosBusca": linha})

        if r.status_code != 200:
            print(f"Erro {r.status_code}: {r.text}")
            return None

        dados = r.json()
        if not dados or not isinstance(dados, list):
            print(f"Linha '{linha}' não encontrada.")
            return None

        # Retorna o código da primeira linha encontrada
        return dados[0].get("cl")

    def obter_previsao(self, codigo_linha):
        """Busca as previsões de chegada para uma linha específica."""
        url = f"{self.base_url}/Previsao/Linha"
        r = self.session.get(url, params={"codigoLinha": codigo_linha}, timeout=30)

        if r.status_code != 200:
            print(f"Falha ao buscar previsões: status {r.status_code}")
            return None

        return r.json()

    def obter_posicoes(self):
        """Busca a posição de todos os veículos em operação, para todas as linhas."""
        url = f"{self.base_url}/Posicao"
        r = self.session.get(url, timeout=30)

        if r.status_code != 200:
            print(f"Falha ao buscar posições: status {r.status_code}")
            return None

        return r.json()
