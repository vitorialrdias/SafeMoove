# SafeMoove — Análise Ponta a Ponta

*Gerado em 2026-08-20, cobrindo o estado do projeto após a reestruturação e o teste real do pipeline.*

## 1. Objetivo do projeto

Extrair dados de ônibus de São Paulo via API Olho Vivo (SPTrans), publicar em Kafka, persistir em S3 como Parquet e disponibilizar via AWS Glue/Athena para responder duas perguntas de negócio:

1. Quais linhas têm maior atraso?
2. Quantos ônibus rodam por dia, por tipo de linha?

## 2. Arquitetura atual

```mermaid
flowchart LR
    API[API Olho Vivo\nSPTrans] --> P1[producer_linhas]
    API --> P2[producer_paradas]
    API --> P3[producer_corredores]
    API --> P4[producer_posicoes]
    API --> P5[producer_previsao]

    P1 --> K[(Kafka)]
    P2 --> K
    P3 --> K
    P4 --> K
    P5 --> K
    K -. tópico sptrans-linhas .-> P5

    K --> C[consumer_s3\nbatch Parquet]
    C --> S3[(S3\nsafe-moove-raw/parquet/)]
    S3 -.. bloqueado por IAM ..-> G[Glue Data Catalog]
    G --> A[Athena]
```

Todos os componentes rodam via `docker-compose` (Kafka + Zookeeper + 5 producers + 1 consumer), cada producer publicando em seu próprio tópico e o `consumer_s3` centralizando a escrita no S3.

## 3. Componentes — estado atual

### 3.1 Producers (`producer/`)

| Producer | Endpoint SPTrans | Status | Observação |
|---|---|---|---|
| `producer_linhas.py` | `/Linha/Buscar` (força bruta por termo) | ✅ funcionando, testado | Descoberta por busca de 10.000 termos numéricos + prefixos; sem endpoint "listar tudo" na API pública. `LINHAS_MAX_ENCONTRADAS` (env, opcional) permite parar cedo — usado no teste. |
| `producer_paradas.py` | `/Parada/Buscar` | ✅ funcionando, testado | Busca por dígitos+letras (36 termos), ~363 paradas em segundos. |
| `producer_corredores.py` | `/Corredor` | ✅ funcionando, testado | Dataset pequeno e estático (7 corredores em SP), roda uma vez e sai. |
| `producer_posicoes.py` | `/Posicao` (bulk) | ✅ funcionando, testado | Um único request retorna **todas** as linhas/veículos ativos (~2.100 linhas, ~11.500 veículos por poll). Loop a cada `POSICOES_POLL_INTERVAL` (default 10s). |
| `producer_previsao.py` | `/Previsao/Linha` (por linha) | ✅ funcionando, testado | Sem endpoint bulk — consome o tópico `sptrans-linhas` pra descobrir todas as linhas e cicla previsão por linha. Achata `{ps:[{vs:[...]}]}` em uma mensagem por (parada, veículo previsto). |
| ~~`producer_velocidade.py`~~ | `/Velocidade` | ❌ **removido** | Endpoint não existe na API pública (404 confirmado ao vivo em várias variações de path). Tópico e referências removidos do projeto. |

Todos os producers compartilham a classe `SPTransAPI` ([producer/api_sptrans.py](producer/api_sptrans.py)) — sessão autenticada com retry/backoff (`urllib3.Retry`, incluindo POST pra re-autenticação) — em vez de duplicar lógica de auth. Todos rodam sob `main()` + `if __name__ == "__main__":`.

### 3.2 Bug crítico corrigido: cascata de execução

`producer/__init__.py` importava todos os `producer_*.py` no nível de módulo. Como o Python sempre executa o `__init__.py` de um pacote antes de qualquer submódulo, rodar `python -m producer.producer_previsao` cascateava para executar **todos os outros producers primeiro** — incluindo o loop de horas do `producer_linhas` e o loop infinito do `producer_posicoes` — antes do producer pedido sequer começar. Corrigido esvaziando o `__init__.py` e envolvendo cada script em `main()` + guard. Confirmado por teste: importar todos os producers agora não dispara nenhum efeito colateral.

### 3.3 Kafka (`shared/kafka_config.py`, `scripts/create_topics.py`)

5 tópicos ativos (`sptrans-linhas`, `sptrans-paradas`, `sptrans-corredores`, `sptrans-posicoes`, `sptrans-previsoes`), partições definidas em [shared/topics.py](shared/topics.py) — 3 partições para os tópicos de alto volume (posições, previsões), 1 para os demais. Bootstrap servers configurável via `KAFKA_BOOTSTRAP_SERVERS` (default `kafka:9092`, resolve dentro da rede do docker-compose).

### 3.4 Consumer / storage (`consumer/consumer_s3.py`)

Reescrito nesta sessão: em vez de 1 arquivo JSON por mensagem Kafka (problema clássico de "small files" pra Athena), agora:

- Buffer em memória **por tópico**.
- Flush pra um único arquivo Parquet em S3 quando atinge `S3_BATCH_SIZE` (default 500) registros OU `S3_BATCH_INTERVAL_SECONDS` (default 60) desde o último flush — o que vier primeiro.
- Schema fixo por tópico ([shared/schemas.py](shared/schemas.py), sem pandas — só `pyarrow`), evitando inferência de dtype divergente entre batches.
- Commit de offset **restrito às partições recém-persistidas** (nunca `commit()` genérico) — protege contra perda de dados de outros tópicos com buffer ainda não gravado.
- Trata `SIGTERM`/`SIGINT` com flush final de tudo que sobrou no buffer antes de encerrar — **testado ao vivo**: `docker compose stop` disparou o handler, logou "Sinal 15 recebido..." e gravou os batches parciais de posições (523 registros) e previsões (77 registros) antes de fechar.

Layout no S3: `s3://safe-moove-raw/parquet/{topico}/ano=YYYY/mes=MM/dia=DD/{timestamp}.parquet` (mês/dia sempre com 2 dígitos, zero à esquerda).

## 4. Validação real (teste ponta a ponta em 2026-08-05)

Rodado via `docker compose up` com Kafka e S3 reais (bucket `safe-moove-raw`, conta AWS `511758682808`), usando `LINHAS_MAX_ENCONTRADAS=30` pra não esperar a descoberta completa (uma única busca por `"1"` já trouxe 216 linhas reais).

| Tópico | Registros gravados | Arquivos Parquet |
|---|---:|---:|
| linhas | 216 | 1 |
| corredores | 7 | 1 |
| paradas | 363 | 1 |
| posições | 241.358 | 436 |
| previsões | 2.189 | 14 |

Nenhum erro de aplicação nos logs de nenhum serviço. As duas perguntas de negócio foram testadas com esses dados reais, localmente via `pyarrow` (join `posicoes ⋈ linhas`, e um proxy de atraso por deriva de previsão) — confirmando que o **schema atual suporta ambas as análises**, com uma ressalva importante (seção 6).

Os 453 arquivos desse teste continuam no bucket (o usuário optou por manter, não apagar).

## 5. Camada de analytics (Glue/Athena) — em andamento

**Status: bloqueado por permissão IAM.**

- Bucket de resultados do Athena criado: `s3://safemoove-athena-results-511758682808/`.
- Script pronto (`setup_glue.py`, fora do repo, na área de scratch da sessão) para criar o database `safemoove` e as 5 tabelas externas via Athena DDL, com partition projection já configurada corretamente (`projection.enabled`, `type=integer`, `range`, `digits=2` pra bater com o zero-padding de `mes=`/`dia=` no S3, `storage.location.template`).
- Tentativa de execução falhou: usuário IAM `vitoria_dev` não tinha permissão nem de `athena:StartQueryExecution` nem de `glue:CreateDatabase` — e nem de listar suas próprias políticas anexadas (`iam:List*` também negado).
- Usuário informou ter acabado de anexar permissão — **reexecução do script é o próximo passo pendente**, ainda não confirmado.

## 6. Limitações conhecidas

1. **"Atraso" não é uma métrica direta.** A API Olho Vivo não expõe horário programado — só previsão em tempo real (`horario_previsto`) e posição GPS. O que foi validado é um **proxy**: comparar como o horário previsto pra um mesmo (linha, parada, veículo) muda entre leituras sucessivas — se a previsão empurra pra frente, é sinal de atraso crescente. Funciona como mecanismo, mas pra um número confiável de "atraso real" seria necessário ou (a) coletar por uma janela bem mais longa pra deriva acumular, ou (b) cruzar com o GTFS estático da SPTrans como referência de horário programado — nenhum dos dois está implementado.
2. **`producer_linhas` é força bruta.** Sem endpoint "listar todas as linhas" na API pública, a descoberta completa (10.000 termos numéricos + prefixos) leva horas. O teste real usou uma amostra (216 de ~2.100+ linhas vistas via `/Posicao`) — em produção, rodar sem `LINHAS_MAX_ENCONTRADAS` é necessário pra cobertura completa, e isso afeta diretamente quantas linhas o `producer_previsao` consegue cobrir (ele só cicla sobre o que está no tópico `sptrans-linhas`).
3. **At-least-once, não exactly-once.** Se `put_object` no S3 tiver sucesso mas o commit de offset falhar/crashar logo depois, o próximo restart reprocessa esse lote inteiro — pode gerar linhas duplicadas em Parquet (até um batch inteiro, no pior caso). Não há deduplicação na camada de analytics ainda; se necessário, precisaria de uma chave natural (linha+veículo+timestamp) e `ROW_NUMBER()`/`DISTINCT` no Athena.
4. **`producer_velocidade` foi removido, não substituído.** Se a métrica de velocidade por trecho for necessária no futuro, teria que ser derivada de `posicoes` (distância entre leituras consecutivas de um mesmo veículo / tempo decorrido), não existe endpoint dedicado na API pública.
5. **Pipeline nunca rodou em produção contínua.** O teste real foi de ~5 minutos. Comportamento de longo prazo (retenção do Kafka, crescimento do número de arquivos Parquet por dia, custo de storage/scan no Athena, estabilidade do `producer_previsao` ciclando sobre milhares de linhas) ainda não foi observado.
6. **Camada de analytics incompleta.** Tabelas Glue ainda não criadas (bloqueio de IAM, seção 5) — as duas perguntas de negócio só foram validadas via script Python ad-hoc, não via Athena.

## 7. Próximos passos

1. Confirmar que a permissão IAM anexada cobre Glue + Athena e reexecutar `setup_glue.py`.
2. Rodar `producer_linhas` sem limite (ou com um limite bem mais alto) pra ter cobertura real de linhas antes de qualquer análise "de verdade".
3. Decidir a estratégia de atraso: proxy por deriva de previsão (mais simples, já validado) vs. ingestão do GTFS estático como tabela de referência (mais correto, mais trabalho).
4. Definir uma janela de coleta contínua (ex: rodar o pipeline por um dia inteiro) antes de tirar conclusões de negócio — os números atuais são de uma amostra de minutos.
5. Considerar deduplicação na camada de consulta (Athena) dado o modelo at-least-once do consumer.
