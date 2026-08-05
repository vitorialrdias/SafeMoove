import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from shared.logger import get_logger

logger = get_logger(__name__)


class SPTransAPI:

    def __init__(self, token):
        self.token = token
        self.base_url = "http://api.olhovivo.sptrans.com.br/v2.1"
        self.session = requests.Session()

        # allowed_methods inclui POST (o default do Retry exclui POST,
        # o que deixaria a re-autenticação sem retry em falhas transitórias)
        retries = Retry(
            total=3,
            backoff_factor=0.5,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods={"GET", "POST"},
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

        self.autenticar()

    def autenticar(self):
        """Autentica na API e armazena os cookies na sessão."""
        url = f"{self.base_url}/Login/Autenticar"

        # Passa data={} para evitar erro '411 Length Required'
        response = self.session.post(url, params={"token": self.token}, data={})

        if response.status_code != 200 or response.text.strip().lower() != "true":
            logger.error(f"Falha na autenticação SPTrans: status {response.status_code} - {response.text}")
            return False

        return True

    def buscar_linhas(self, termo):
        """Busca linhas por termo (número, letreiro, prefixo etc.)."""
        url = f"{self.base_url}/Linha/Buscar"
        r = self.session.get(url, params={"termosBusca": termo}, timeout=30)

        if r.status_code != 200:
            logger.warning(f"Erro ao buscar linhas '{termo}': status {r.status_code}")
            return None

        dados = r.json()
        return dados if isinstance(dados, list) else None

    def obter_codigo_linha(self, linha):
        dados = self.buscar_linhas(linha)

        if not dados:
            logger.warning(f"Linha '{linha}' não encontrada.")
            return None

        # Retorna o código da primeira linha encontrada
        return dados[0].get("cl")

    def buscar_paradas(self, termo):
        """Busca paradas por termo (nome, endereço etc.)."""
        url = f"{self.base_url}/Parada/Buscar"
        r = self.session.get(url, params={"termosBusca": termo}, timeout=30)

        if r.status_code != 200:
            logger.warning(f"Erro ao buscar paradas '{termo}': status {r.status_code}")
            return None

        dados = r.json()
        return dados if isinstance(dados, list) else None

    def listar_corredores(self):
        """Lista todos os corredores de ônibus."""
        url = f"{self.base_url}/Corredor"
        r = self.session.get(url, timeout=30)

        if r.status_code != 200:
            logger.warning(f"Falha ao buscar corredores: status {r.status_code}")
            return None

        return r.json()

    def obter_previsao(self, codigo_linha):
        """Busca as previsões de chegada para uma linha específica."""
        url = f"{self.base_url}/Previsao/Linha"
        r = self.session.get(url, params={"codigoLinha": codigo_linha}, timeout=30)

        if r.status_code != 200:
            logger.warning(f"Falha ao buscar previsões: status {r.status_code}")
            return None

        return r.json()

    def obter_posicoes(self):
        """Busca a posição de todos os veículos em operação, para todas as linhas."""
        url = f"{self.base_url}/Posicao"
        r = self.session.get(url, timeout=30)

        if r.status_code != 200:
            logger.warning(f"Falha ao buscar posições: status {r.status_code}")
            return None

        return r.json()
