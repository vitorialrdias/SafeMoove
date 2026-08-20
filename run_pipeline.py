import subprocess

PRODUCERS = [
    "producer.producer_linhas",
    "producer.producer_posicoes",
    "producer.producer_previsao",
]

print("Iniciando pipeline SPTrans...")

processos = []

for p in PRODUCERS:

    print(f"Subindo {p}")

    proc = subprocess.Popen(
        ["python", "-m", p]
    )

    processos.append(proc)

# mantém rodando
for proc in processos:
    proc.wait()