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

        # allowed_methods inclui POST — o default do Retry o exclui, o que
        # deixaria a re-autenticação sem retry
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
        url = f"{self.base_url}/Login/Autenticar"

        # Passa data={} para evitar erro '411 Length Required'
        response = self.session.post(url, params={"token": self.token}, data={})

        if response.status_code != 200 or response.text.strip().lower() != "true":
            logger.error(f"Falha na autenticação SPTrans: status {response.status_code} - {response.text}")
            return False

        return True

    def buscar_linhas(self, termo):
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

        return dados[0].get("cl")

    def obter_previsao(self, codigo_linha):
        url = f"{self.base_url}/Previsao/Linha"
        r = self.session.get(url, params={"codigoLinha": codigo_linha}, timeout=30)

        if r.status_code != 200:
            logger.warning(f"Falha ao buscar previsões: status {r.status_code}")
            return None

        return r.json()

    def obter_posicoes(self):
        url = f"{self.base_url}/Posicao"
        r = self.session.get(url, timeout=30)

        if r.status_code != 200:
            logger.warning(f"Falha ao buscar posições: status {r.status_code}")
            return None

        return r.json()
