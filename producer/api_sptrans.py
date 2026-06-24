import requests


class SPTransAPI:

    def __init__(self, token):

        self.token = token

        self.headers = {
            "Authorization": f"Bearer {token}",
            "Accept": "application/json"
        }

    def obter_codigo_linha(self, linha):

        url = (
            "https://homolog.gateway.apilib.prefeitura.sp.gov.br/"
            "sptrans/olhovivo/v2.1/Linha/Buscar"
        )

        r = requests.get(
            url,
            headers=self.headers,
            params={"termosBusca": linha}
        )

        if r.status_code != 200:
            print(
                f"Erro {r.status_code}: {r.text}"
            )
            return None

        return r.json()[0]["cl"]

    def obter_posicao(self, codigo_linha):

        url = (
            "https://homolog.gateway.apilib.prefeitura.sp.gov.br/"
            "sptrans/olhovivo/v2.1/Posicao/Linha"
        )

        r = requests.get(
            url,
            headers=self.headers,
            params={"codigoLinha": codigo_linha}
        )

        if r.status_code != 200:
            print(
                f"Erro {r.status_code}: {r.text}"
            )
            return None

        return r.json()
